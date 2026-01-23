import asyncio

from db.factory import AsyncDatabaseFactory


async def main() -> None:
    print("Hello from price-flow!")
    await AsyncDatabaseFactory.get_manager()
    print("Init database")


if __name__ == "__main__":
    asyncio.run(main())
