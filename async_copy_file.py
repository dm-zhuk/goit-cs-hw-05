#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Асинхронний скрипт для сортування файлів за розширеннями.
Копіює файли з вихідної папки у цільову, розподіляючи їх по підпапках
на основі розширення.
"""

from aiopath import AsyncPath

import aiofiles
import asyncio
import argparse
import logging
import sys


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Парсинг аргументів командного рядка."""
    parser = argparse.ArgumentParser(
        description="Асинхронне сортування файлів за розширеннями у підпапки."
    )
    parser.add_argument("source", type=str, help="Шлях до вихідної папки")
    parser.add_argument(
        "destination",
        type=str,
        nargs="?",
        default="dist",
        help="Шлях до цільової папки (за замовчуванням: './dist')",
    )
    return parser.parse_args()


async def copy_file(file_path: AsyncPath, dest_root: AsyncPath) -> None:
    """
    Асинхронно копіює файл у підпапку, названу за розширенням.

    Args:
        file_path: Шлях до вихідного файлу
        dest_root: Коренева цільова папка
    """
    try:
        suffix = file_path.suffix.lower()
        ext_folder_name = suffix[1:] if suffix else "no_extension"

        ext_dir = dest_root / ext_folder_name
        await ext_dir.mkdir(parents=True, exist_ok=True)

        dest_file = ext_dir / file_path.name

        async with aiofiles.open(file_path, "rb") as src:
            content = await src.read()
        async with aiofiles.open(dest_file, "wb") as dst:
            await dst.write(content)

        logger.info(f"Скопійовано: {file_path.name} → {ext_dir.name}/")

    except Exception as e:
        logger.error(f"Помилка при копіюванні {file_path}: {e}")


async def read_folder(source_path: AsyncPath, dest_root: AsyncPath) -> None:
    """Рекурсивно читає всі файли у папці та копіює їх."""
    try:
        async for item in source_path.iterdir():
            if await item.is_dir():
                await read_folder(item, dest_root)
            elif await item.is_file():
                await copy_file(item, dest_root)
    except PermissionError:
        logger.error(f"Немає доступу до папки: {source_path}")
    except Exception as e:
        logger.error(f"Помилка при обробці {source_path}: {e}")


async def main() -> None:
    """Головна асинхронна функція."""
    start_time = asyncio.get_event_loop().time()

    args = parse_arguments()
    source = AsyncPath(args.source)
    destination = AsyncPath(args.destination)

    if not await source.exists():
        logger.error(f"Вихідна папка не існує: {source}")
        sys.exit(1)
    if not await source.is_dir():
        logger.error(f"Вказаний шлях не є папкою: {source}")
        sys.exit(1)

    await destination.mkdir(parents=True, exist_ok=True)
    logger.info(f"Цільова папка: {destination.resolve()}")

    await read_folder(source, destination)

    elapsed = asyncio.get_event_loop().time() - start_time
    logger.info(f"Готово! Виконано за {elapsed:.2f} сек.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Скрипт зупинено користувачем.")
        sys.exit(0)


# Test sample:
# python3 async_copy_file.py "/Users/macbook/Documents/Travel/Peru" "/Users/macbook/Documents/Travel/Brasil"
