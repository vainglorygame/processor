#!/usr/bin/node
/* jshint esnext:true */
"use strict";

/* processor inserts API data into the database.
 * It listens to the queue `process` and expects a JSON
 * which is a match structure or a player structure.
 * It will forward notifications to web.
 */

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    Seq = require("sequelize"),
    api_name_mappings = require("../orm/mappings").map;

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    QUEUE = process.env.QUEUE || "process",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    // matches + players, 5 players with 50 matches as default
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 5 * (50 + 1),
    // maximum number of elements to be inserted in one statement
    CHUNKSIZE = parseInt(process.env.CHUNKSIZE) || 100,
    MAXCONNS = parseInt(process.env.MAXCONNS) || 10,  // how many concurrent actions
    DOANALYZEMATCH = process.env.DOANALYZEMATCH == "true",
    ANALYZE_QUEUE = process.env.ANALYZE_QUEUE || "analyze",
    LOAD_TIMEOUT = parseFloat(process.env.LOAD_TIMEOUT) || 5000, // ms
    IDLE_TIMEOUT = parseFloat(process.env.IDLE_TIMEOUT) || 700;  // ms

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: true,
            colorize: true
        })
    ]
});

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "processor", QUEUE],
        json: true
    });

// helpers
const camelCaseRegExp = new RegExp(/([a-z])([A-Z]+)/g);
function camelToSnake(text) {
    return text.replace(camelCaseRegExp, (m, $1, $2) =>
        $1 + "_" + $2.toLowerCase());
}

// MadGlory API uses snakeCase, our db uses camel_case
function snakeCaseKeys(obj) {
    Object.keys(obj).forEach((key) => {
        const new_key = camelToSnake(key);
        if (new_key == key) return;
        obj[new_key] = obj[key];
        delete obj[key];
    });
    return obj;
}

// split an Set() into arrays of max chunksize
function* chunks(data) {
    const arr = [...data];  // TODO maybe slice the Set?
    for (let c=0, len=arr.length; c<len; c+=CHUNKSIZE)
        yield arr.slice(c, c+CHUNKSIZE);
}

// helper to convert API response into flat JSON
// db structure is (almost) 1:1 the API structure
// so we can insert the flat API response as-is
function flatten(obj) {
    const attrs = obj.attributes || {},
        stats = attrs.stats || {};
    let o = Object.assign({}, obj, attrs, stats);
    o.api_id = o.id;  // rename
    delete o.id;
    delete o.type;
    delete o.attributes;
    delete o.stats;
    delete o.relationships;
    return snakeCaseKeys(o);
}

amqp.connect(RABBITMQ_URI).then(async (rabbit) => {
    global.process.on("SIGINT", () => {
        rabbit.close();
        global.process.exit();
    });

    // connect to rabbit & db
    const seq = new Seq(DATABASE_URI, {
        logging: false,
        pool: {
            max: MAXCONNS
        }
    });

    const ch = await rabbit.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    await ch.assertQueue(QUEUE + "_failed", { durable: true });
    await ch.assertQueue(ANALYZE_QUEUE, { durable: true });
    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    const model = require("../orm/model")(seq, Seq);

    logger.info("configuration", {
        QUEUE, BATCHSIZE, CHUNKSIZE, MAXCONNS, LOAD_TIMEOUT, IDLE_TIMEOUT,
        DOANALYZEMATCH, ANALYZE_QUEUE
    });

    // performance logging
    let load_timer = undefined,
        idle_timer = undefined,
        profiler = undefined;

    // Maps to quickly convert API names to db ids
    let item_db_map = new Map(),      // "Halcyon Potion" to id
        hero_db_map = new Map(),      // "*SAW*" to id
        series_db_map = new Map(),    // date to series id
        game_mode_db_map = new Map(), // "ranked" to id
        role_db_map = new Map();      // "captain" to id

    // populate maps
    await Promise.all([
        model.Item.findAll()
            .map((item) => item_db_map.set(item.name, item.id)),
        model.Hero.findAll()
            .map((hero) => hero_db_map.set(hero.name, hero.id)),
        model.Series.findAll()
            .map((series) => {
                if (series.dimension_on == "player")
                    series_db_map.set(series.name, series.id);
            }),
        model.GameMode.findAll()
            .map((mode) => game_mode_db_map.set(mode.name, mode.id)),
        model.Role.findAll()
            .map((role) => role_db_map.set(role.name, role.id))
    ]);
    if (item_db_map.size == 0 ||
        hero_db_map.size == 0 ||
        series_db_map.size == 0 ||
        game_mode_db_map.size == 0 ||
        role_db_map == 0) {
        logger.error("mapping tables are not seeded!!! quitting");
        global.process.exit();
    }

    // buffers that will be filled until BATCHSIZE is reached
    // to make db transactions more efficient
    let player_data = new Set(),
        match_data = new Set(),
        msg_buffer = new Set();

    ch.consume(QUEUE, async (msg) => {
        switch (msg.properties.type) {
            case "player":
                // bridge sends a single object
                let player = JSON.parse(msg.content);
                // player objects that arrive here came from a search
                // with search, bridge can't update last_update
                player.last_update = seq.fn("NOW");
                player_data.add(player);
                msg_buffer.add(msg);
                break;
            case "match":
                // apigrabber sends a single object
                const match = JSON.parse(msg.content);
                // deduplicate and reject immediately
                if (await model.Match.count({ where: { api_id: match.id } }) > 0) {
                    logger.info("duplicate match", match.id);
                    if (msg.properties.headers.notify) {
                        await ch.publish("amq.topic",
                            msg.properties.headers.notify,
                            new Buffer("matches_dupe"));
                        // send match_dupe to web player.ign.api_id
                        await ch.publish("amq.topic",
                            msg.properties.headers.notify + "." + match.id,
                            new Buffer("match_dupe"));
                    }
                    await ch.nack(msg, false, false);
                } else if (match.rosters.length < 2 || match.rosters[0].id == "null")  {
                    logger.info("invalid match", match.id);
                    await ch.publish("amq.topic",
                        msg.properties.headers.notify + "." + match.id,
                        new Buffer("match_invalid"));
                    // it is really `"null"`.
                    // reject invalid matches (handling API bugs)
                    await ch.nack(msg, false, false);
                    await ch.sendToQueue(QUEUE + "_failed", msg.content, {
                        persistent: true,
                        type: msg.properties.type,
                        headers: msg.properties.headers
                    });
                } else {
                    // all good
                    match_data.add(match);
                    msg_buffer.add(msg);
                }
                break;
        }

        // fill queue until batchsize or idle
        // for logging of the time between batch fill and batch process
        if (profiler == undefined) profiler = logger.startTimer();
        // timeout after first job
        if (load_timer == undefined)
            load_timer = setTimeout(tryProcess, LOAD_TIMEOUT);
        // timeout after last job
        if (idle_timer != undefined)
            clearTimeout(idle_timer);
        idle_timer = setTimeout(tryProcess, IDLE_TIMEOUT);
        // maximum data pressure
        if (match_data.size + player_data.size == BATCHSIZE)
            await tryProcess();
    }, { noAck: false });

    // wrap process() in message handler
    async function tryProcess() {
        const msgs = new Set(msg_buffer);
        msg_buffer.clear();

        profiler.done("buffer filled");
        profiler = undefined;

        logger.info("processing batch", {
            messages: msgs.size,
            players: player_data.size,
            matches: match_data.size
        });

        // clean up to allow processor to accept while we wait for db
        clearTimeout(idle_timer);
        clearTimeout(load_timer);
        idle_timer = undefined;
        load_timer = undefined;

        if (msgs.size == 0) {
            logger.info("nothing to do");
            return;
        }
        const player_objects = new Set(player_data),
            match_objects = new Set(match_data);
        player_data.clear();
        match_data.clear();

        try {
            await process(player_objects, match_objects);

            logger.info("acking batch", { size: msgs.size });
            await Promise.map(msgs, async (m) => await ch.ack(m));

            // notify web
            await Promise.map(msgs, async (m) => {
                if (m.properties.headers.notify == undefined) return;
                switch (m.properties.type) {
                    // new match
                    case "match":
                        await ch.publish("amq.topic", m.properties.headers.notify,
                            new Buffer("match_update"));
                        break;
                    case "player":
                        // player obj updated
                        await ch.publish("amq.topic", m.properties.headers.notify,
                            new Buffer("stats_update"));
                        break;
                }
            });
            // …global about new matches
            if (match_objects.length > 0)
                await ch.publish("amq.topic", "global", new Buffer("matches_update"));
            // notify follow up services
            if (DOANALYZEMATCH)
                await Promise.each(match_objects, async (m) =>
                    await ch.sendToQueue(ANALYZE_QUEUE, new Buffer(m.id),
                        { persistent: true }));
        } catch (err) {
            if (err instanceof Seq.TimeoutError ||
                (err instanceof Seq.DatabaseError && err.errno == 1213)) {
                // deadlocks / timeout
                logger.error("SQL error", err);
                await Promise.map(msgs, async (m) =>
                    await ch.nack(m, false, true));  // retry
            } else {
                // log, move to error queue and NACK
                logger.error(err);
                await Promise.map(msgs, async (m) => {
                    await ch.sendToQueue(QUEUE + "_failed", m.content, {
                        persistent: true,
                        type: m.properties.type,
                        headers: m.properties.headers
                    });
                    await ch.nack(m, false, false);
                });
            }
        }
    }

    // finish a whole batch
    async function process(player_objects, match_objects) {
        // aggregate record objects to do a bulk insert
        let match_records = new Set(),
            roster_records = new Set(),
            participant_records = new Set(),
            participant_stats_records = new Set(),
            participant_items_records = new Set(),
            players = new Map(),
            player_records = new Set(),
            player_records_dates = new Set(),  // from /players
            asset_records = new Set();

        // populate `_records`
        // data from `/players`
        player_objects.forEach((p) => {
            let player = flatten(p);
            player.created_at = new Date(Date.parse(player.created_at));
            player.last_match_created_date = player.created_at;

            logger.info("processing player",
                { name: player.name, region: player.shard_id });
            if (!players.has(player.api_id)) {
                players.set(player.api_id, player);
            } else {  // or a player object that is more recent than the buffer's
                if (players.get(player.api_id).created_at < player.created_at) {
                    logger.info("buffer has same more recent direct player object, overwriting");
                    players.set(player.api_id, player);
                }
            }
        });

        // data from `/matches`
        match_objects.forEach((match) => {
            match.createdAt = new Date(Date.parse(match.createdAt));

            // flatten jsonapi nested response into our db structure-like shape
            // also, push missing fields
            match.rosters = match.rosters.map((roster) => {
                roster.matchApiId = match.id;
                // TODO backwards compatibility, all objects have shardId since May 10th
                roster.attributes.shardId = roster.attributes.shardId || match.attributes.shardId;
                roster.createdAt = match.createdAt;
                // TODO API workaround: roster does not have `winner`
                if (roster.participants.length > 0)
                    roster.attributes.stats.winner = roster.participants[0].stats.winner;
                else  // Blitz 2v0, see 095e86e4-1bd3-11e7-b0b1-0297c91b7699 on eu
                    roster.attributes.stats.winner = false;

                roster.participants = roster.participants.map((participant) => {
                    // ! attributes added here need to be added via `calculate_participant_stats` too
                    participant.attributes.shardId = participant.attributes.shardId || roster.attributes.shardId;
                    participant.rosterApiId = roster.id;
                    participant.matchApiId = match.id;
                    participant.createdAt = roster.createdAt;
                    participant.playerApiId = participant.player.id;

                    // API bug fixes (TODO)
                    // items on AFK is `null` not `{}`
                    participant.attributes.stats.itemGrants = participant.attributes.stats.itemGrants || {};
                    participant.attributes.stats.itemSells = participant.attributes.stats.itemSells || {};
                    participant.attributes.stats.itemUses = participant.attributes.stats.itemUses || {};
                    // jungle_kills is `null` in BR
                    participant.attributes.stats.jungleKills = participant.attributes.stats.jungleKills || 0;

                    // map items: names/id -> name -> db
                    const item_id = ((i) => item_db_map.get(api_name_mappings.get(i)));
                    let itms = [];

                    const pas = participant.attributes.stats;  // I'm lazy

                    participant.participantItems = {};
                    let ppi = participant.participantItems;

                    // Map for dynamic columns, participant_items table
                    // Map { item id: count }
                    const items = new Map();
                    pas.items.forEach((i, idx) => {
                        // if this is the first occurence of the item…
                        if (pas.items.findIndex((_i) => _i == i) == idx)
                            // …set id ->…
                            items.set(item_id(i),
                                // …count(*).
                                pas.items.filter((_i) => _i == i).length
                            )
                    });
                    ppi.items = items;

                    const itemGrants = new Map();
                    Object.entries(pas.itemGrants).forEach(([i, cnt]) =>
                        itemGrants.set(item_id(i), cnt));
                    ppi.item_grants = itemGrants;

                    const itemUses = new Map();
                    Object.entries(pas.itemUses).forEach(([i, cnt]) =>
                        itemUses.set(item_id(i), cnt));
                    ppi.item_uses = itemUses;

                    const itemSells = new Map();
                    Object.entries(pas.itemSells).forEach(([i, cnt]) =>
                        itemSells.set(item_id(i), cnt));
                    ppi.item_sells = itemSells;

                    // csv for backwards compatibility (TODO)
                    pas.items = pas.items.map((i) => item_id(i).toString()).join(",");
                    // csv with count seperated by ;
                    pas.itemGrants = Object.keys(pas.itemGrants)
                        .map((key) => item_id(key) + ";" + pas.itemGrants[key]).join(",");
                    pas.itemUses = Object.keys(pas.itemUses)
                        .map((key) => item_id(key) + ";" + pas.itemUses[key]).join(",");
                    pas.itemSells = Object.keys(pas.itemSells)
                        .map((key) => item_id(key) + ";" + pas.itemSells[key]).join(",");

                    participant.player.attributes.shardId = participant.player.attributes.shardId
                        || participant.attributes.shardId;
                    if (participant.player.attributes.createdAt != undefined)
                        participant.player.attributes.createdAt =
                            new Date(Date.parse(participant.player.attributes.createdAt));
                    else participant.player.attributes.created_at = participant.createdAt;

                    // `flatten` will only flatten what is in `.attributes` & meta data
                    ppi = flatten(ppi);
                    participant.player = flatten(participant.player);
                    return flatten(participant);
                });
                return flatten(roster);
            });
            match.assets = match.assets.map((asset) => {
                asset.matchApiId = match.id;
                asset.attributes.shardId = asset.attributes.shardId || match.attributes.shardId;
                return flatten(asset);
            });
            match = flatten(match);

            // after conversion, create the array of records
            match_records.add(match);
            match.rosters.forEach((r) => {
                roster_records.add(r);
                r.participants.forEach((p) => {
                    // splits participant into `participant`, `participant_stats` and `participant_items`
                    const p_pstats = calculate_participant_stats(match, r, p),
                        part = p_pstats[0],
                        pstats = p_pstats[1],
                        pitems = p_pstats[2];
                    if (pitems.items.size > 0)
                        pitems.items = Seq.fn("COLUMN_CREATE",
                            [].concat(...pitems.items.entries()));
                    else pitems.items = "";

                    if (pitems.item_grants.size > 0)
                        pitems.item_grants = Seq.fn("COLUMN_CREATE",
                            [].concat(...pitems.item_grants.entries()));
                    else pitems.item_grants = "";

                    if (pitems.item_uses.size > 0)
                        pitems.item_uses = Seq.fn("COLUMN_CREATE",
                            [].concat(...pitems.item_uses.entries()));
                    else pitems.item_uses = "";

                    if (pitems.item_sells.size > 0)
                        pitems.item_sells = Seq.fn("COLUMN_CREATE",
                            [].concat(...pitems.item_sells.entries()));
                    else pitems.item_sells = "";

                    // push split records
                    participant_records.add(part);
                    participant_stats_records.add(pstats);
                    participant_items_records.add(pitems);

                    // if match.included has an unknown player
                    if (!players.has(p.player.api_id))
                        players.set(p.player.api_id, p.player);
                    else {
                        // or a player object that is more recent than the buffer's
                        if (players.get(p.player.api_id).created_at < p.player.created_at) {
                            if (players.get(p.player.api_id).last_update != undefined) {
                                logger.info("buffer has same more recent indirect player object, overwriting direct");
                                // indirect overwrites direct's stats; keep last_update and lmcd
                                p.player.last_update = players.get(p.player.api_id).last_update;
                                p.player.last_match_created_date = players.get(p.player.api_id).last_match_created_date;
                            }
                            // else direct/indirect overwrites indirect
                            players.set(p.player.api_id, p.player);
                        }
                    }
                });
            });
            match.assets.forEach((a) => asset_records.add(a));
        });

        // player.last_update = last time bridge ran an update (!= undefined for /players objects)
        // player.last_match_created_date = last match from a full history fetch (as above)
        // player.created_at = recency of the player object (on every object, always >= lmcd)
        // last_update and lmcd must not be overwritten
        await Promise.map(players.values(), async (player) => {
            const count = await model.Player.count({ where: {
                api_id: player.api_id,
                created_at: { $gt: player.created_at }
            } });
            // update requested from bridge sets both created_at and last_update
            if (player.last_update == undefined) {
                // this will not overwrite last_update and created_at
                if (count == 0) player_records.add(player);
                // else db is more recent than buffer, skip
            } else {
                // this will overwrite dates
                player_records_dates.add(player);
            }
        });

        let transaction_profiler = logger.startTimer();
        // now access db
        // upsert whole batch in parallel
        logger.info("inserting batch into db");
        await seq.transaction({ autocommit: false }, async (transaction) => {
            await Promise.map(chunks(match_records), async (m_r) =>
                model.Match.bulkCreate(m_r, {
                    ignoreDuplicates: true,  // if this happens, something is wrong
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
            await Promise.map(chunks(roster_records), async (r_r) =>
                model.Roster.bulkCreate(r_r, {
                    ignoreDuplicates: true,
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
            await Promise.map(chunks(participant_records), async (p_r) =>
                model.Participant.bulkCreate(p_r, {
                    ignoreDuplicates: true,
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
            await Promise.map(chunks(participant_stats_records), async (p_s_r) =>
                model.ParticipantStats.bulkCreate(p_s_r, {
                    ignoreDuplicates: true,
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
            await Promise.map(chunks(participant_items_records), async (p_i_r) =>
                model.ParticipantItems.bulkCreate(p_i_r, {
                    ignoreDuplicates: true,
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
            await Promise.map(chunks(player_records), async (p_r) =>
                model.Player.bulkCreate(p_r, {
                    fields: [
                        // specify fields or Sequelize attempts to update all fields
                        "created_at",
                        "api_id", "name", "shard_id",
                        "skill_tier",
                        "level", "lifetime_gold", "xp"
                    ],
                    updateOnDuplicate: [
                        "created_at",
                        "api_id", "name", "shard_id",
                        "skill_tier",
                        "level", "lifetime_gold", "xp"
                    ],
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
            await Promise.map(chunks(player_records_dates), async (p_r_d) =>
                model.Player.bulkCreate(p_r_d, {
                    fields: [
                        "last_update", "last_match_created_date",
                        "created_at",
                        "api_id", "name", "shard_id",
                        "skill_tier",
                        "level", "lifetime_gold", "xp"
                    ],
                    updateOnDuplicate: [
                        "last_update", "last_match_created_date",
                        "created_at",
                        "api_id", "name", "shard_id",
                        "skill_tier",
                        "level", "lifetime_gold", "xp"
                    ],
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
            await Promise.map(chunks(asset_records), async (a_r) =>
                model.Asset.bulkCreate(a_r, {
                    ignoreDuplicates: true,
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
        });
        transaction_profiler.done("database transaction");
    }

    // Split participant API data into participant and participant_stats
    // Should not need to query db here.
    function calculate_participant_stats(match, roster, participant) {
        let p_s = {},  // participant_stats_record
            p_i = {},  // participant_items record (dynamic columns, csv is on p_s [TODO])
            p = {};  // participant_record

        // copy all values that are required in db `participant` to `p`/`p_s`/`p_i` here
        // items - simple, has been prepared in `process()` already (grep for `ppi`)
        p_i = participant.participant_items;
        // not really "item", but I need this one
        p_i.surrendered = (match.end_game_reason == "surrender" && participant.winner == false);
        p_i.participant_api_id = participant.api_id;

        // meta
        p_s.participant_api_id = participant.api_id;
        p_s.final = true;  // these are the stats at the end of the match
        p_s.updated_at = new Date();
        p_s.created_at = new Date(Date.parse(match.created_at));
        p_s.created_at.setMinutes(p_s.created_at.getMinutes() + match.duration / 60);
        p_s.items = participant.items;
        p_s.item_grants = participant.item_grants;
        p_s.item_uses = participant.item_uses;
        p_s.item_sells = participant.item_sells;
        p_s.duration = match.duration;

        p.created_at = match.created_at;
        // mappings
        // hero names additionally need to be mapped old to new names
        // (Sayoc = Taka)
        p.hero_id = hero_db_map.get(api_name_mappings.get(participant.actor));
        if (match.patch_version != "")
            p.series_id = series_db_map.get("Patch " + match.patch_version);
        else {
            if (p_s.created_at < new Date("2017-03-28T15:00:00"))
                p.series_id = series_db_map.get("Patch 2.2");
            else if (p_s.created_at < new Date("2017-04-26T15:00:00"))
                p.series_id = series_db_map.get("Patch 2.3");
            else p.series_id = series_db_map.get("Patch 2.4");
        }
        p.game_mode_id = game_mode_db_map.get(match.game_mode);

        // attributes to copy from API to participant
        // these don't change over the duration of the match
        // (or aren't in Telemetry)
        ["api_id", "shard_id", "player_api_id", "roster_api_id", "match_api_id",
            "winner", "went_afk", "first_afk_time",
            "skin_key", "skill_tier", "level",
            "karma_level", "actor"].map((attr) =>
                p[attr] = participant[attr]);

        // attributes to copy from API to participant_stats
        // with Telemetry, these will be calculated in intervals
        ["kills", "deaths", "assists", "minion_kills",
            "jungle_kills", "non_jungle_minion_kills",
            "crystal_mine_captures", "gold_mine_captures",
            "kraken_captures", "turret_captures",
            "gold", "farm"].map((attr) =>
                p_s[attr] = participant[attr]);

        let role = classify_role(p_s);

        // score calculations
        let impact_score = 50;
        switch (role) {
            case "carry":
                impact_score = -0.47249153 + 0.50145197 * p_s.assists - 0.7136091 * p_s.deaths + 0.18712844 * p_s.kills + 0.00531455 * p_s.farm;
                p_s.nacl_score = 1 * p_s.kills + 0.5 * p_s.assists + 0.03 * p_s.farm + 5 * p.winner;
                break;
            case "jungler":
                impact_score = -0.54510754 + 0.19982097 * p_s.assists - 0.35694721 * p_s.deaths + 0.09942473 * p_s.kills + 0.01256313 * p_s.farm;
                p_s.nacl_score = 1 * p_s.kills + 0.5 * p_s.assists + 0.04 * p_s.farm + 5 * p.winner;
                break;
            case "captain":
                impact_score = -0.46473539 + 0.09968104 * p_s.assists - 0.38401479 * p_s.deaths + 0.14753133 * p_s.kills + 0.03431293 * p_s.farm;
                p_s.nacl_score = 1 * p_s.kills + 1 * p_s.assists + 5 * p.winner;
                break;
        }
        p_s.impact_score = (impact_score - (-4.5038622921659375) ) / (4.431094119937388 - (-4.5038622921659375) );


        // classifications
        p.role_id = role_db_map.get(role);

        // traits calculations
        if (roster.hero_kills == 0) p_s.kill_participation = 0;
        else p_s.kill_participation = (p_s.kills + p_s.assists) / roster.hero_kills;

        return [p, p_s, p_i];
    }

    // return "captain" "carry" "jungler"
    function classify_role(participant_stats) {
        const is_captain_score = 2.34365487 + (-0.06188674 * participant_stats.non_jungle_minion_kills) + (-0.10575069 * participant_stats.jungle_kills),  // about 88% accurate, trained on Hero.is_captain
            is_carry_score = -1.88524473 + (0.05593593 * participant_stats.non_jungle_minion_kills) + (-0.0881661 * participant_stats.jungle_kills),  // about 90% accurate, trained on Hero.is_carry
            is_jungle_score = -0.78327066 + (-0.03324596 * participant_stats.non_jungle_minion_kills) + (0.10514832 * participant_stats.jungle_kills);  // about 88% accurate
        if (is_captain_score > is_carry_score && is_captain_score > is_jungle_score)
            return "captain";
        if (is_carry_score > is_jungle_score)
            return "carry";
        return "jungler";
    }
});

process.on("unhandledRejection", (err) => {
    logger.error(err);
    global.process.exit(1);  // fail hard and die
});
