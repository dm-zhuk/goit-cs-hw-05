from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

import string
import requests
import matplotlib.pyplot as plt
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        logger.error(f"Download error: {e}")
        return None


# Функція для видалення знаків пунктуації
def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word):
    return word, 1


def shuffle_function(mapped):
    shuffled = defaultdict(list)
    for key, value in mapped:
        shuffled[key].append(value)
    return shuffled


def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


# Виконання MapReduce
def map_reduce(text, num_workers=8):
    # Видалення знаків пунктуації
    text = remove_punctuation(text)
    words = text.split()

    # 1. MAP: Паралельний Мапінг
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        mapped = list(executor.map(map_function, words))

    # 2. SHUFFLE
    shuffled = shuffle_function(mapped)

    # 3. REDUCE: Паралельна Редукція
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        reduced = list(executor.map(reduce_function, shuffled.items()))

    return dict(reduced)


# Фільтр: тільки слова з 6+ букв
def filter_functional_words(word_counts, min_length=6):
    return {word: cnt for word, cnt in word_counts.items() if len(word) >= min_length}


# Видобування топ-N слів (Top 10 most frequent words)
def top_words(word_counts, top_n=10):
    avoided = filter_functional_words(word_counts)
    return sorted(avoided.items(), key=lambda x: x[1], reverse=True)[:top_n]


def visualize_top_words(top_words, title="10 Most Frequent words"):
    words, counts = zip(*top_words)

    plt.figure(figsize=(10, 6))
    y_pos = range(len(words))
    plt.barh(y_pos, counts, align="center", color="#1f77b4")
    plt.yticks(y_pos, words)
    plt.gca().invert_yaxis()
    plt.xlabel("Frequency")
    plt.title(title)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Вхідний текст для обробки
    url = "https://gutenberg.net.au/ebooks06/0604171.txt"

    logger.info("Завантаження тексту …")
    text = get_text(url)
    if not text:
        raise SystemExit("Помилка: Не вдалося отримати вхідний текст.")

    logger.info("Очищення знаків пунктуації …")
    cleaned = remove_punctuation(text)

    logger.info("Виконання Map-Reduce …")
    counts = map_reduce(cleaned, num_workers=12)

    logger.info("Видобування топ-10 …")
    top10 = top_words(counts, top_n=10)

    logger.info("10 Most Frequent words:")
    for word, cnt in top10:
        logger.info(f"  {word:<15} {cnt}")

    logger.info("Побудова графіка …")
    visualize_top_words(top10, title="Top 10 most frequent words")
