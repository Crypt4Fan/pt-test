import asyncio
import re
import aiohttp
from aio_pika import connect_robust
from aio_pika.patterns import RPC


LINK_PATTERN = re.compile(r'((href)|(src))\s*=\s*[\'"](?P<link>[^\'"]*?)[\'"]')


def get_links(html, base_url):
    links = set()
    for match in re.finditer(LINK_PATTERN, html):
        link = match.group('link')
        if not link.startswith('http'):
            link = str(base_url) + link
        links.add(link)
    return links


async def harvest(target_url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(target_url) as response:
                html = await response.text()
                links = get_links(html, response.url)
                result = {'success': True, 'links': list(links)}
    except:
        result = {'success': False}
    return result


async def worker():
    connection = await connect_robust('amqp://guest:guest@127.0.0.1/')
    channel = await connection.channel()
    rpc = await RPC.create(channel)
    await rpc.register('harvest', harvest, auto_delete=True)
    return connection


def main():
    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(worker())
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection.close())
        loop.shutdown_asyncgens()


if __name__ == "__main__":
    main()
