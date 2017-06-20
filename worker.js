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
    api_name_mappings = require("../orm/mappings").map,
    isAbility = require("../orm/mappings").isAbility,
    isItem = require("../orm/mappings").isItem,
    isHero = require("../orm/mappings").isHero,
    sleep = require("sleep-promise");

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

// split an array into arrays of max chunksize
function* chunks(arr) {
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

// main code
(async () => {
    let seq, model, rabbit, ch;

    // connect to rabbit & db
    while (true) {
        try {
            seq = new Seq(DATABASE_URI, {
                logging: false,
                max: MAXCONNS
            });
            rabbit = await amqp.connect(RABBITMQ_URI, { heartbeat: 120 });
            ch = await rabbit.createChannel();
            await ch.assertQueue(QUEUE, {durable: true});
            break;
        } catch (err) {
            logger.error("Error connecting", err);
            await sleep(5000);
        }
    }

    model = require("../orm/model")(seq, Seq);

    // performance logging
    let load_timer = undefined,
        idle_timer = undefined,
        profiler = undefined;

    // Maps to quickly convert API names to db ids
    let item_db_map = new Map(),      // "Halcyon Potion" to id
        hero_db_map = new Map(),      // "*SAW*" to id
        series_db_map = new Map(),    // date to series id
        game_mode_db_map = new Map(), // "ranked" to id
        role_db_map = new Map(),      // "captain" to id
        hero_role_map = new Map();    // SAW.id to "carry"

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

    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    // buffers that will be filled until BATCHSIZE is reached
    // to make db transactions more efficient
    let player_data = new Set(),
        match_data = new Set(),
        telemetry_data = new Set(),
        msg_buffer = new Set();

    ch.consume(QUEUE, async (msg) => {
        if (msg.properties.type == "player") {
            // bridge sends a single object
            player_data.add(JSON.parse(msg.content));
            msg_buffer.add(msg);
        }
        if (msg.properties.type == "match") {
            // apigrabber sends a single object
            const match = JSON.parse(msg.content);
            // deduplicate and reject immediately
            if (await model.Match.count({ where: { api_id: match.id } }) > 0) {
                if (msg.properties.headers.notify != undefined) {
                    await ch.publish("amq.topic",
                        msg.properties.headers.notify,
                        new Buffer("matches_dupe"))
                    // send match_dupe to web player.ign.api_id
                    await ch.publish("amq.topic",
                        msg.properties.headers.notify + "." + match.id,
                        new Buffer("match_dupe"))
                }
            } else match_data.add(match);
            msg_buffer.add(msg);
        }
        if (msg.properties.type == "telemetry") {
            telemetry_data.add(JSON.parse(msg.content));
            msg_buffer.add(msg);
        }

        // fill queue until batchsize or idle
        // for logging of the time between batch fill and batch process
        if (profiler == undefined) profiler = logger.startTimer();
        // timeout after first job
        if (load_timer == undefined)
            load_timer = setTimeout(process, LOAD_TIMEOUT);
        // timeout after last job
        if (idle_timer != undefined)
            clearTimeout(idle_timer);
        idle_timer = setTimeout(process, IDLE_TIMEOUT);
        // maximum data pressure
        if (match_data.size + player_data.size + telemetry_data.size == BATCHSIZE)
            await process();
    }, { noAck: false });

    // finish a whole batch
    async function process() {
        profiler.done("buffer filled");
        profiler = undefined;

        logger.info("processing batch", {
            players: player_data.size,
            matches: match_data.size,
            telemetries: telemetry_data.size
        });

        // clean up to allow processor to accept while we wait for db
        clearTimeout(idle_timer);
        clearTimeout(load_timer);
        const player_objects = new Set(player_data),
            match_objects = new Set(match_data),
            telemetry_objects = new Set(telemetry_data),
            msgs = new Set(msg_buffer);
        idle_timer = undefined;
        load_timer = undefined;
        player_data.clear();
        match_data.clear();
        telemetry_data.clear();
        msg_buffer.clear();

        const processed_players = new Set(); // to sort out duplicates

        // aggregate record objects to do a bulk insert
        let match_records = [],
            roster_records = [],
            participant_records = [],
            participant_stats_records = [],
            participant_phase_records = [],  // Telemetry
            player_records = [],
            player_records_direct = [],  // via `/players`
            asset_records = [];

        // populate `_records`
        // data from `/players`
        // `each` executes serially so there are
        // no race conditions within one batch
        await Promise.each(player_objects, async (p) => {
            const player = flatten(p);
            if (processed_players.has(player.api_id)) {
                // duplicate within one batch
                logger.warn("got player in additional region",
                    { name: player.name, region: player.shard_id });
                const duplicate = player_records_direct.find((pr) =>
                    pr.api_id == player.api_id);
                if (duplicate == undefined) return;  // TODO wtf???
                if (duplicate.created_at < player.created_at) {
                    // replace by newer one as below
                    player_records_direct.splice(
                        player_records_direct.indexOf(duplicate), 1);
                } else {
                    logger.warn("ignoring player from additional region",
                        { name: player.name, region: player.shard_id });
                    return;
                }
            } else {
                processed_players.add(player.api_id);
            }
            // player objects that arrive here came from a search
            // with search, updater can't update last_update
            player.last_update = seq.fn("NOW");
            logger.info("processing player",
                { name: player.name, region: player.shard_id });
            // duplicate in batch and db
            // check whether there is a player in db
            // that has a more recent `created_at`
            // this is only the case with region changes
            const count = await model.Player.count({ where: {
                api_id: player.api_id,
                created_at: {
                    $gt: player.created_at // equal: just update last_update
                }
            }});
            if (count > 0) {
                logger.warn("ignoring player who seems to have switched from region",
                    { name: player.name, region: player.shard_id });
                return;
            } else player_records_direct.push(player);
        });

        // reject invalid matches (handling API bugs) TODO should happen in apigrabber
        match_objects.forEach((match, idx) => {
            // it is really `"null"`.
            if (match.rosters[0].id == "null") delete match_objects[idx];
        });

        // data from `/matches`
        match_objects.forEach((match) => {
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
                    // csv
                    participant.attributes.stats.items =
                        pas.items.map((i) => item_id(i).toString()).join(",");
                    // csv with count seperated by ;
                    participant.attributes.stats.itemGrants =
                        Object.keys(pas.itemGrants)
                            .map((key) => item_id(key) + ";" + pas.itemGrants[key]).join(",");
                    participant.attributes.stats.itemUses =
                        Object.keys(pas.itemUses)
                            .map((key) => item_id(key) + ";" + pas.itemUses[key]).join(",");
                    participant.attributes.stats.itemSells =
                        Object.keys(pas.itemSells)
                            .map((key) => item_id(key) + ";" + pas.itemSells[key]).join(",");

                    participant.player.attributes.shardId = participant.player.attributes.shardId || participant.attributes.shardId;
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
            match_records.push(match);
            match.rosters.forEach((r) => {
                roster_records.push(r);
                r.participants.forEach((p) => {
                    const p_pstats = calculate_participant_stats(match, r, p);
                    // participant gets split into participant and p_stats
                    participant_records.push(p_pstats[0]);
                    participant_stats_records.push(p_pstats[1]);
                    // deduplicate player
                    // in a batch, it is very likely that players are duplicated
                    // so this improves performance a bit
                    if (!processed_players.has(p.player.api_id)) {
                        processed_players.add(p.player.api_id);
                        player_records.push(p.player);
                    }
                });
            });
            match.assets.forEach((a) => asset_records.push(a));
        });


        // data from Telemetry, one phase (early/mid/late/…) per obj
        await Promise.map(telemetry_objects, async (telemetry) => {
            if (telemetry.data.length == 0) return;  // TODO rm me
            // api -> telemetry format
            const sideToTeam = (s) => s == "left/blue"? "Left" : "Right",
                // yes there is yet another format and yes it's strings
                sideToTeamNo = (s) => s == "left/blue"? "1" : "2";
            // get match participant references
            const participants = await model.Participant.findAll({
                where: { match_api_id: telemetry.match_api_id },
                include: [ {  // TODO rm once pushed to participant
                    model: model.Roster,
                    attributes: [ "side" ]
                } ]
            }).map((p) => { return {
                api_id: p.api_id,
                player_api_id: p.player_api_id,
                actor: p.actor,
                team: sideToTeam(p.roster.side),
                teamNo: sideToTeamNo(p.roster.side)
            } });

            // seconds since epoch; first spawn time
            const matchstart = new Date(Date.parse(telemetry.match_start)).getTime() / 1000;

            // link participant <-> Telemetry actor/target
            // available as `.actor` or as `.target`
            telemetry.data.forEach((t) => {
                // seconds after this phase's start
                t.offset = new Date(Date.parse(t.time)).getTime() / 1000 - matchstart;

                // linking
                if (t.type == "HeroSelect")
                    t.actor = participants.filter((p) =>
                        p.player_api_id == t.payload.Player)[0];
                if (t.type == "BuyItem"
                    || t.type == "SellItem"
                    || t.type == "UseItemAbility"
                    || t.type == "LearnAbility"
                    || t.type == "UseAbility"
                    || t.type == "LevelUp")
                    t.actor = participants.filter((p) =>
                        p.actor == t.payload.Actor
                        && p.team == t.payload.Team)[0];
                if (t.type == "UseItemAbility"
                    || t.type == "UseAbility")
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.TargetActor
                        && p.team != t.payload.Team)[0];
                // damage actor
                if ((t.type == "DealDamage"
                     || t.type == "KillActor")
                    && t.payload.IsHero == 1)
                    t.actor = participants.filter((p) =>
                        p.actor == t.payload.Actor
                        && p.team == t.payload.Team)[0];
                // damage target
                if (t.type == "DealDamage"
                    && t.payload.TargetIsHero == 1)
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.Target
                        && p.team != t.payload.Team)[0];
                // kill target
                if (t.type == "KillActor"
                    && t.payload.TargetIsHero == 1)
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.Killed
                        && p.team == t.payload.KilledTeam)[0];
            });
            /*
            telemetry.data.forEach((ev) => {  // TODO debug
                if (ev.payload.Ability == undefined || ev.payload.IsHero == 0) return;
                if (!api_name_mappings.has(ev.payload.Ability))
                    console.error("ab to name map missing", ev.payload.Ability);
            });
            */
            const participants_phase = participants.map((p) => { return {
                // TODO ban data workaround
                start: telemetry.start < 0? 0 : telemetry.start,  // in seconds
                end: telemetry.end,
                participant_api_id: p.api_id,

                kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.IsHero == 1
                    && ev.payload.TargetIsHero == 1
                ).length,
                deaths: telemetry.data.filter((ev) =>
                    ev.target == p
                    && ev.type == "KillActor"
                    && ev.payload.TargetIsHero == 1
                ).length,
                // assists missing in data
                minion_kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ["*JungleMinion_TreeEnt*",
                        "*Neutral_JungleMinion_DefaultBig*",
                        "*Neutral_JungleMinion_DefaultSmall*",
                        "*LeadMinion*",
                        "*RangedMinion*",
                        "*TankMinion*"
                    ].indexOf(ev.payload.Killed) != -1
                ).length,
                jungle_kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ["*JungleMinion_TreeEnt*",
                        "*Neutral_JungleMinion_DefaultBig*",
                        "*Neutral_JungleMinion_DefaultSmall*"
                    ].indexOf(ev.payload.Killed) != -1
                ).length,
                non_jungle_minion_kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ["*LeadMinion*",
                        "*RangedMinion*",
                        "*TankMinion*"
                    ].indexOf(ev.payload.Killed) != -1
                ).length,
                crystal_mine_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.Killed == "*JungleMinion_CrystalMiner*"
                ).length,
                gold_mine_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.Killed == "*JungleMinion_GoldMiner*"
                ).length,
                kraken_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.Killed == "*Kraken_Jungle*"
                ).length,
                turret_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && (ev.payload.Killed == "*Turret*"
                        || ev.payload.Killed == "*VainTurret*")
                ).length,
                // TODO Telemetry does not give accurate LifetimeGold
                gold: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "LevelUp"
                    && ev.payload.LifetimeGold > acc
                    ? ev.payload.LifetimeGold
                    : acc
                , null),
                dmg_true_hero: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 1
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_kraken: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ["*Kraken_Jungle*",
                        "*Kraken_Captured*"
                    ].indexOf(ev.payload.Target) != -1
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*Turret*"
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_vain_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*VainTurret*"
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_others: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 0
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_dealt_hero: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 1
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_kraken: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ["*Kraken_Jungle*",
                        "*Kraken_Captured*"
                    ].indexOf(ev.payload.Target) != -1
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*Turret*"
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_vain_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*VainTurret*"
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_others: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 0
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_rcvd_dealt_hero: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 1
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_rcvd_true_hero: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 1
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_rcvd_dealt_others: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 0
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_rcvd_true_others: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 0
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                hero_level: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "LevelUp"
                    && ev.payload.Level > acc
                    ? ev.payload.Level
                    : acc
                , -1),
                items: null,  // TODO
                item_grants: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "BuyItem"
                    ? acc.concat(item_db_map.get(api_name_mappings.get(ev.payload.Item)))
                    : acc
                , [])),
                item_sells: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "SellItem"
                    ? acc.concat(item_db_map.get(api_name_mappings.get(ev.payload.Item)))
                    : acc
                , [])),
                ability_levels: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "LearnAbility"
                    ? acc.concat([ [ api_name_mappings.get(ev.payload.Ability).split(" ")[1],
                        ev.offset ] ])
                    : acc
                , [])),
                ability_uses: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "UseAbility"
                    && ["A", "B", "C"].indexOf(
                        api_name_mappings.get(ev.payload.Ability).split(" ")[1]
                    ) != -1
                    ? acc.concat([ [ api_name_mappings.get(ev.payload.Ability).split(" ")[1],
                        ev.offset ] ])
                    : acc
                , [])),
                ability_damage: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 1
                    && isAbility(ev.payload.Source)  // TODO
                    && api_name_mappings.has(ev.payload.Source)
                    && ["A", "B", "C"].indexOf(
                        api_name_mappings.get(ev.payload.Source).split(" ")[1]
                    ) != -1  // TODO refactor here
                    ? acc.concat([ [ api_name_mappings.get(ev.payload.Source).split(" ")[1],
                          ev.payload.Damage, ev.offset ] ])
                    : acc
                , [])),
                item_uses: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "UseItemAbility"
                    ? acc.concat([ [ item_db_map.get(api_name_mappings.get(ev.payload.Ability)),
                          ev.offset ] ])
                    : acc
                , [])),
                player_damage: null,  // TODO
                items: null,  // TODO
                draft_position: telemetry.data.filter((ev) =>
                    ev.type == "HeroSelect").indexOf(
                        telemetry.data.filter((ev) =>
                            ev.type == "HeroSelect"
                            && ev.actor == p)[0]),
                ban: hero_db_map.get(api_name_mappings.get(
                    telemetry.data
                        .filter((ev) => ev.type == "HeroBan" &&
                            ev.payload.Team == p.teamNo)
                        .map((sel) => sel.payload.Hero)[0]  // can be null
                )),
                pick: hero_db_map.get(api_name_mappings.get(
                    telemetry.data
                        .filter((ev) =>
                            ev.type == "HeroSelect"
                            && ev.actor == p)
                        .map((sel) => sel.payload.Hero)[0]  // can be null
                )),// traits calculated later
            } });
            participant_phase_records = participant_phase_records.concat(
                participants_phase);  // TODO calc stats
        });

        let transaction_profiler = logger.startTimer();
        // now access db
        try {
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
                await Promise.map(chunks(participant_phase_records), async (p_p_r) =>
                    model.ParticipantPhases.bulkCreate(p_p_r, {
                        /* ignoreDuplicates: true, TODO DEBUG */
                        ignoreDuplicates: false,
                        updateOnDuplicate: [],
                        transaction: transaction
                    }), { concurrency: MAXCONNS }
                );
                await Promise.map(chunks(player_records), async (pl_r) =>
                    model.Player.bulkCreate(pl_r, {
                        ignoreDuplicates: true,
                        transaction: transaction
                    }), { concurrency: MAXCONNS }
                );
                await Promise.map(chunks(player_records_direct), async (p_r_d) =>
                    model.Player.bulkCreate(player_records_direct, {
                        // if set to [] (all), upsert messes with autoincrement
                        updateOnDuplicate: [
                            "shard_id", "api_id", "name", "last_update",
                            "created_at", "level", "xp", "lifetime_gold",
                            "skill_tier"
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

            logger.info("acking batch", { size: msgs.size });
            await Promise.map(msgs, async (m) => await ch.ack(m));
        } catch (err) {
            // this should only happen for Deadlocks in prod
            // it *must not* fail due to broken schema or missing dependency
            // TODO: eliminate such cases earlier in the chain
            // and immediately NACK those broken matches, requeueing only the rest
            logger.error("SQL error", err);
            await Promise.map(msgs, async (m) => await ch.nack(m, false, true));
            return;  // give up
        }
        transaction_profiler.done("database transaction");

        // notify web
        await Promise.map(msgs, async (m) => {
            if (m.properties.headers.notify == undefined) return;
            let notif = "error";
            // new match
            if (m.properties.type == "match") {
                notif = "matches_update";
                // TODO this sends match_update for every match in the batch to every player
                // notify player.name.api_id about match_update
                await Promise.map(match_records, async (mat) =>
                    await ch.publish("amq.topic",
                        m.properties.headers.notify + "." + mat.api_id,
                        new Buffer("match_update"))
                );
                await ch.publish("amq.topic", m.properties.headers.notify,
                    new Buffer("match_update"));
            }
            // player obj updated
            if (m.properties.type == "player") notif = "stats_update";
            // new phases
            if (m.properties.type == "telemetry") {
                notif = "phases_update";
                // notify player.name.api_id about phase_update
                await Promise.map(telemetry_objects, async (t) =>
                    await ch.publish("amq.topic",
                        m.properties.headers.notify + "." + t.match_api_id,
                        new Buffer("phase_update"))
                );
            }

            if (m.properties.headers.donotify == true)  // TODO remove later
                await ch.publish("amq.topic", m.properties.headers.notify,
                    new Buffer(notif));
        });
        // …global about new matches
        if (match_records.length > 0)
            await ch.publish("amq.topic", "global", new Buffer("matches_update"));
        // notify follow up services
        if (DOANALYZEMATCH)
            await Promise.each(match_records, async (m) =>
                await ch.sendToQueue(ANALYZE_QUEUE, new Buffer(m.api_id),
                    { persistent: true }));
    }

    // Split participant API data into participant and participant_stats
    // Should not need to query db here.
    function calculate_participant_stats(match, roster, participant) {
        let p_s = {},  // participant_stats_record
            p = {};  // participant_record

        // copy all values that are required in db `participant` to `p`/`p_s` here
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

        return [p, p_s];
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
})();

process.on("unhandledRejection", function(reason, promise) {
    logger.error(reason);
});
