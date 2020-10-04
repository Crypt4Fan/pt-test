import uuid
import asyncio
from databases import Database
from aiohttp import web
from aio_pika import connect_robust
from aio_pika.patterns import RPC
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, ForeignKey
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ENUM, UUID


DSN = 'postgresql://postgres:postgres@127.0.0.1/postgres'
DB_METADATA = MetaData()
STATUS_ENUM = ENUM(
    'PENDING',
    'IN PROGRESS',
    'SUCCESS',
    'FAILED',
    name='status_enum',
    metadata=DB_METADATA
)
TASKS_TABLE = Table(
    'tasks',
    DB_METADATA,
    Column('id', UUID(as_uuid=True), primary_key=True),
    Column('target_url', String, nullable=False),
    Column('status', STATUS_ENUM)
)
RESULTS_TABLE = Table(
    'results',
    DB_METADATA,
    Column('id', Integer, primary_key=True),
    Column(
        'task_id',
        UUID(as_uuid=True),
        ForeignKey('tasks.id', ondelete='CASCADE', onupdate='CASCADE')
    ),
    Column('link', String, nullable=False)
)


async def send_task_to_worker(app, task_id, target_url):

    query = TASKS_TABLE.update().where(TASKS_TABLE.c.id==task_id)
    values = {'status': 'IN PROGRESS'}
    await app['db'].execute(query=query, values=values)
    result = await app['queue'].call('harvest', kwargs={'target_url': target_url})

    success = result.get('success', False)
    if success:
        async with app['db'].transaction():

            query = RESULTS_TABLE.insert()
            values = [{'task_id': task_id, 'link': link} for link in result['links']]
            await app['db'].execute_many(query=query, values=values)

            query = TASKS_TABLE.update().where(TASKS_TABLE.c.id==task_id)
            values = {'status': 'SUCCESS'}
            await app['db'].execute(query=query, values=values)

    else:
        query = TASKS_TABLE.update().where(TASKS_TABLE.c.id==task_id)
        values = {'status': 'FAILED'}
        await app['db'].execute(query=query, values=values)


async def create_task(request):

    data = await request.json()
    target_url = data.get('target_url', '')
    if target_url:
        task_id = uuid.uuid4()

        query = TASKS_TABLE.insert()
        values = {'id': task_id, 'target_url': target_url, 'status': 'PENDING'}

        await request.app['db'].execute(query=query, values=values)

        asyncio.create_task(send_task_to_worker(request.app, task_id, target_url))

        return web.json_response({'task_id': str(task_id)}, status=201)
    else:
        return web.json_response(
            {'error': 'Unsupported or missing target_url parametr'},
            status=400
        )


async def get_task_info(request):

    try:
        task_id = uuid.UUID(request.match_info['id'].lower())
    except:
        return web.json_response({'error': 'Incorrect task_id'}, status=400)

    query = select([TASKS_TABLE.c.status, TASKS_TABLE.c.target_url]).\
        where(TASKS_TABLE.c.id==task_id)
    result = await request.app['db'].fetch_one(query=query)

    if result:
        if result['status'] == 'SUCCESS':
            links = []
            query = select([RESULTS_TABLE.c.link]).\
                select_from(TASKS_TABLE.join(RESULTS_TABLE)).\
                where(TASKS_TABLE.c.id==task_id)
            async for row in request.app['db'].iterate(query=query):
                links.append(row['link'])
            return web.json_response({
                'task_id': str(task_id),
                'target_url': result['target_url'],
                'status': result['status'],
                'links': links
            })
        else:
            return web.json_response({
                'task_id': str(task_id),
                'target_url': result['target_url'],
                'status': result['status'],
            })
    else:
        return web.json_response({'error': 'Task doesn\'t exist'}, status=400)


async def delete_task(request):
    try:
        task_id = uuid.UUID(request.match_info['id'].lower())
    except:
        return web.json_response({'error': 'Incorrect task_id'}, status=400)

    query = select([TASKS_TABLE.c.id, TASKS_TABLE.c.target_url]).\
        where(TASKS_TABLE.c.id==task_id)
    result = await request.app['db'].fetch_one(query=query)

    if result:
        query = TASKS_TABLE.delete().where(TASKS_TABLE.c.id==task_id)
        await request.app['db'].execute(query=query)
        return web.json_response({
                'task_id': str(task_id),
                'target_url': result['target_url'],
                'status': 'DELETED',
            })
    else:
        return web.json_response({'error': 'Task doesn\'t exist'}, status=400)


async def on_start(app):

    engine = create_engine(DSN)
    DB_METADATA.create_all(engine)

    app['db'] = Database(DSN)
    await app['db'].connect()

    connection = await connect_robust('amqp://guest:guest@127.0.0.1/')
    channel = await connection.channel()
    app['queue'] = await RPC.create(channel)


async def on_exit(app):

    await app['db'].disconnect()


def main():

    controller = web.Application()
    controller.on_startup.append(on_start)
    controller.on_shutdown.append(on_exit)

    controller.add_routes([
        web.post('/tasks', create_task),
        web.get(r'/tasks/{id}', get_task_info),
        web.delete(r'/tasks/{id}', delete_task)
    ])

    web.run_app(controller)


if __name__ == "__main__":
    main()
