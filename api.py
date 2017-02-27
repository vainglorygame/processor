#!/usr/bin/python3

import asyncio
import os
import glob
import logging
import json
import asyncpg

import joblib.joblib


source_db = {
    "host": os.environ.get("POSTGRESQL_SOURCE_HOST") or "vaindock_postgres_raw",
    "port": os.environ.get("POSTGRESQL_SOURCE_PORT") or 5532,
    "user": os.environ.get("POSTGRESQL_SOURCE_USER") or "vainraw",
    "password": os.environ.get("POSTGRESQL_SOURCE_PASSWORD") or "vainraw",
    "database": os.environ.get("POSTGRESQL_SOURCE_DB") or "vainsocial-raw"
}

dest_db = {
    "host": os.environ.get("POSTGRESQL_DEST_HOST") or "vaindock_postgres_web",
    "port": os.environ.get("POSTGRESQL_DEST_PORT") or 5432,
    "user": os.environ.get("POSTGRESQL_DEST_USER") or "vainweb",
    "password": os.environ.get("POSTGRESQL_DEST_PASSWORD") or "vainweb",
    "database": os.environ.get("POSTGRESQL_DEST_DB") or "vainsocial-web"
}


class Worker(object):
    def __init__(self):
        self._queue = None
        self._srcpool = None
        self._destpool = None
        self._queries = {}

    async def connect(self, sourcea, desta):
        """Connect to database."""
        logging.info("connecting to database")
        self._queue = joblib.joblib.JobQueue()
        await self._queue.connect(**sourcea)
        await self._queue.setup()
        self._srcpool = await asyncpg.create_pool(**sourcea)
        self._destpool = await asyncpg.create_pool(**desta)

    async def setup(self):
        """Initialize the database."""
        scriptroot = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        for path in glob.glob(scriptroot + "/queries/*.sql"):
            # utf-8-sig is used by pgadmin, doesn't hurt to specify
            # file names: raw target table
            table = os.path.splitext(os.path.basename(path))[0]
            with open(path, "r", encoding="utf-8-sig") as file:
                self._queries[table] = file.read()
                logging.info("loaded query '%s'", table)
        logging.info("creating index")
        async with self._destpool.acquire() as con:
            async with con.transaction():
                await con.execute("CREATE UNIQUE INDEX ON match(\"apiId\")")
                await con.execute("CREATE UNIQUE INDEX ON player(\"apiId\")")

    async def _execute_job(self, jobid, payload):
        """Finish a job."""
        object_id = payload["id"]
        explicit_player = payload["playername"]
        async with self._srcpool.acquire() as srccon:
            async with self._destpool.acquire() as destcon:
                logging.debug("%s: processing '%s'", jobid, object_id)
                async with srccon.transaction():
                    async with destcon.transaction():
                        # 1 object in raw : n objects in web
                        for table, query in self._queries.items():
                            logging.debug("%s: running '%s' query",
                                          jobid, table)
                            # fetch from raw, converted to format for web table
                            if table == "player":
                                # upsert under special conditions
                                data = await srccon.fetchrow(
                                    query, object_id, explicit_player)
                                await self._playerinto(destcon, data, table)
                            else:
                                data = await srccon.fetchrow(
                                    query, object_id)
                                # insert processed result into web table
                                await self._into(destcon, data, table)
                    data = await srccon.fetchrow(
                        "DELETE FROM match WHERE id=$1", object_id)

    async def _playerinto(self, conn, data, table):
        """Insert a player named tuple into a table.
        Upserts specific columns."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = """
            INSERT INTO player ("{0}") VALUES ({1})
            ON CONFLICT("apiId") DO UPDATE SET ("{0}") = ({1})
            WHERE player."lastMatchCreatedDate" < EXCLUDED."lastMatchCreatedDate"
        """.format(
            "\", \"".join(keys), ", ".join(placeholders))
        await conn.execute(query, (*data))

    async def _into(self, conn, data, table):
        """Insert a named tuple into a table."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = "INSERT INTO {} (\"{}\") VALUES ({}) ON CONFLICT DO NOTHING".format(
            table, "\", \"".join(keys), ", ".join(placeholders))
        await conn.execute(query, (*data))

    async def _work(self):
        """Fetch a job and run it."""
        jobid, payload, _ = await self._queue.acquire(jobtype="process")
        if jobid is None:
            raise LookupError("no jobs available")
        logging.debug("%s: starting job", jobid)
        await self._execute_job(jobid, payload)
        await self._queue.finish(jobid)
        logging.debug("%s: finished job", jobid)

    async def run(self):
        """Start jobs forever."""
        while True:
            try:
                await self._work()
            except LookupError:
                logging.info("nothing to do, idling")
                await asyncio.sleep(10)


async def startup():
    for _ in range(1):
        worker = Worker()
        await worker.connect(
            source_db, dest_db
        )
        await worker.setup()
        await worker.run()

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
