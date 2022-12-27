import asyncio
import datetime

from more_itertools import chunked
from config import URL_SWAPI
from aiohttp import ClientSession
from db import Session, engine, Base, People

CHUNK_SIZE = 10
URL = URL_SWAPI


async def get_person(person_id):
    session = ClientSession()
    response = await session.get(f'{URL}/{person_id}')
    status_code = response.status
    person = await response.json()
    await session.close()
    return person, status_code


async def get_people(start, end):
    for id_chunk in chunked(range(start, end), CHUNK_SIZE):
        coroutines = [get_person(i) for i in id_chunk]
        people = await asyncio.gather(*coroutines)
        for person in people:
            yield person


async def paste_people(persons):
    async with Session() as session:
        people_orm = []

        for person in persons:
            if person[1] == 404:
                continue
            else:
                new_person = People(
                    birth_year=person[0]['birth_year'],
                    eye_color=person[0]['eye_color'],
                    films=','.join(person[0]['films']),
                    gender=person[0]['gender'],
                    hair_color=person[0]['hair_color'],
                    height=person[0]['height'],
                    homeworld=person[0]['homeworld'],
                    mass=person[0]['mass'],
                    name=person[0]['name'],
                    skin_color=person[0]['skin_color'],
                    species=','.join(person[0]['species']),
                    starships=','.join(person[0]['starships']),
                    vehicles=','.join(person[0]['vehicles'])
                )
                people_orm.append(new_person)
        session.add_all(people_orm)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    person_buffer = []

    async for person in get_people(1, 101):
        person_buffer.append(person)
        if len(person_buffer) >= CHUNK_SIZE:
            asyncio.create_task(paste_people(person_buffer))
            person_buffer = []

    if person_buffer:
        await paste_people(person_buffer)

    tasks = set(asyncio.all_tasks())
    tasks = tasks - {asyncio.current_task()}
    for task in tasks:
        await task

    await engine.dispose()

if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
