from dataclasses import dataclass
from datetime import date, datetime, timedelta
from re import search
from typing import List, Union

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
from requests import get as get_request


@dataclass
class NewsItem:
    title: str
    abstract: str
    image_src: str
    content: str
    published_at: Union[datetime, None]
    url: str

    def dict(self):
        if isinstance(self.published_at, datetime):
            published_at = self.published_at.isoformat()
        else:
            published_at = None

        return {
            "title": self.title,
            "abstract": self.abstract,
            "image_src": self.image_src,
            "content": self.content,
            "published_at": published_at,
            "url": self.url
        }


class RIAAgent:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36'
    }

    def __init__(self):
        today = date.today()

        month = today.month
        month = str(month) if len(str(month)) == 2 else f"0{month}"

        day = today.day
        day = str(day) if len(str(day)) == 2 else f"0{day}"

        self.entrypoint = f"https://ria.ru/{today.year}{month}{day}/"

    def _get_request(self, url: str) -> str:
        response = get_request(url, headers=self.headers)
        if response.status_code != 200:
            raise ConnectionError(f"Accessing url failed: {url}")

        return response.text

    def _collect_urls(self):
        response_text = self._get_request(url=self.entrypoint)
        html = BeautifulSoup(response_text, "lxml")
        links_elements = html.select("a.list-item__title")
        links = [
            e.get("href", None) for e in links_elements
            if e.get("href", None) and e.get("href", "").startswith("http")
        ]

        return links

    @staticmethod
    def _get_text(html: BeautifulSoup, selector: str, many=False) -> str:
        text = ""

        if many:
            elements = html.select(selector)
            for element in elements:
                text += f"{element.get_text(strip=True)}\n"
            text = text.strip()
        else:
            element = html.select_one(selector)
            if element:
                text = element.get_text(strip=True)

        return text

    @staticmethod
    def _get_image(html: BeautifulSoup, selector: str) -> str:
        image_src = ""

        element = html.select_one(selector)
        if element:
            image_src = element.get("src", "")

        return image_src

    @staticmethod
    def _get_datetime(html: BeautifulSoup, selector: str) -> datetime:
        pattern = r"\d{2}\:\d{2}\s\d{2}\.\d{2}\.\d{4}"

        element = html.select_one(selector)
        if element:
            datetime_str = element.get_text()
            match = search(pattern=pattern, string=datetime_str)
            if match:
                datetime_str = match.group()

                dt = datetime.strptime(datetime_str, "%H:%M %d.%m.%Y")

                return dt

    def _parse_news_item(self, html: BeautifulSoup, url: str) -> NewsItem:
        news_item = NewsItem(
            title=self._get_text(
                html=html,
                selector="div.article__title"
            ),
            abstract=self._get_text(
                html=html,
                selector="h1.article__second-title"
            ),
            image_src=self._get_image(
                html=html,
                selector="div.photoview__open img"
            ),
            content=self._get_text(
                html=html,
                selector="div.article__block[data-type=\"text\"]",
                many=True
            ),
            published_at=self._get_datetime(
                html=html,
                selector="div.article__info-date"
            ),
            url=url
        )

        return news_item

    def _collect_news_items(self, links) -> List[NewsItem]:
        items = []
        for link in links:
            response_text = self._get_request(url=link)
            html = BeautifulSoup(response_text, "lxml")

            news_item = self._parse_news_item(html=html, url=link)
            if news_item:
                items.append(news_item)

        return items

    def scrape(self):
        links = self._collect_urls()
        news_items = self._collect_news_items(links=links)

        news_items = [i.dict() for i in news_items]

        return news_items


@task
def get_news_items(ti=None):
    agent = RIAAgent()
    news_items = agent.scrape()

    ti.xcom_push(key='news_items', value=news_items)


@task
def store_news_items(ti=None):
    news_items = ti.xcom_pull(task_ids="get_news_items", key="news_items")
    news_items = [
        (
            i["abstract"],
            i["content"],
            i["title"],
            i["image_src"],
            i["published_at"],
            i["url"]
        ) for i in news_items
    ]

    sql_statement = """
    INSERT INTO
        storage.public.stg_news_items (
            abstract, content, title, image_src, published_at, url
        )
    VALUES 
        (%s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

    pg_hook = PostgresHook(
        postgres_conn_id="news_items",
        schema="storage"
    )

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.executemany(sql_statement, news_items)

    pg_conn.commit()
    pg_conn.close()


with DAG(
        "ria-scraper",
        default_args={
            "depends_on_past": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=5)
        },
        description='RIA.ru scraper',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 11),
        catchup=False,
        tags=['scraper']
) as dag:
    store_news_items() << get_news_items()
