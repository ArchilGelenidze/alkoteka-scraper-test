from json import JSONDecodeError
import time

from loguru import logger
import scrapy

from core.logging.set_up_logger import setup_custom_logger

setup_custom_logger()


class AlkotekaSpider(scrapy.Spider):
    name = "alco_spider"
    allowed_domains = ["alkoteka.com"]

    ITEMS_PER_PAGE = 100

    CITY = "Krasnodar"
    CITY_UUID = "4a70f9e0-46ae-11e7-83ff-00155d026416"  # Krasnodar
    INIT_API_URL = f"https://alkoteka.com/web-api/v1/csrf-cookie?city_uuid={CITY_UUID}"

    START_URLS = [
        "https://alkoteka.com/catalog/vino",
        "https://alkoteka.com/catalog/shampanskoe-i-igristoe",
        "https://alkoteka.com/catalog/krepkiy-alkogol",
        "https://alkoteka.com/catalog/slaboalkogolnye-napitki-2",
        "https://alkoteka.com/catalog/enogram",
        "https://alkoteka.com/catalog/axioma-spirits",
        "https://alkoteka.com/catalog/bezalkogolnye-napitki-1",
    ]

    def start_requests(self):
        logger.info(f"Initializing session for {self.CITY}...")

        yield scrapy.Request(
            url=self.INIT_API_URL,
            callback=self.start_parsing
        )

    def start_parsing(self, response):
        if response.status == 204:
            logger.success("Session established successfully! Ready to parse...")

            for url in self.START_URLS:
                category_slug = url.rstrip('/').split("/")[-1]
                page = 1
                api_url = f"https://alkoteka.com/web-api/v1/product?city_uuid={self.CITY_UUID}&page={page}&per_page={self.ITEMS_PER_PAGE}&root_category_slug={category_slug}"

                yield scrapy.Request(
                    url=api_url,
                    callback=self.parse_api_response,
                    meta={
                        "original_category_url": url,
                        "category_slug": category_slug,
                        "page": page
                    }
                )
        else:
            logger.error(f"Failed to initialize session. HTTP Status: {response.status}")

    def parse_api_response(self, response):
        try:
            data = response.json()
        except JSONDecodeError:
            logger.error(f"Failed to decode JSON from {response.url}")
            return

        if not data.get("success"):
            logger.error(f"API returned not success for {response.url}")
            return

        results = data.get("results", [])

        for product in results:
            formated_item = self.format_item(product)
            yield formated_item

        logger.success("Page parsed successfully!")

        meta_data = data.get("meta", {})
        if meta_data.get("has_more_pages"):
            next_page = meta_data.get("current_page", 1) + 1
            category_slug = response.meta["category_slug"]

            # Ограничение на количество страниц
            if next_page > 1:
                return

            next_api_url = f"https://alkoteka.com/web-api/v1/product?city_uuid={self.CITY_UUID}&page={next_page}&per_page={self.ITEMS_PER_PAGE}&root_category_slug={category_slug}"

            yield scrapy.Request(
                url=next_api_url,
                callback=self.parse_api_response,
                meta={
                    "original_category_url": response.meta["original_category_url"],
                    "category_slug": category_slug,
                    "page": next_page
                }
            )

    def format_item(self, raw_data: dict) -> dict:
        metadata = {
            "__description": "",
            "Артикул": str(raw_data.get("vendor_code", ""))
        }

        volume = ""
        color = ""

        for label in raw_data.get("filter_labels", []):
            title = label.get("title")
            filter_name = label.get("filter")

            if not title or not filter_name:
                continue

            metadata[filter_name.capitalize()] = title

            if filter_name == "obem":
                volume = title
            elif filter_name == "cvet":
                color = title

        raw_title = raw_data.get("name", "")
        title_parts = [raw_title]

        if volume and volume.lower() not in raw_title.lower():
            title_parts.append(volume)
        if color and color.lower() not in raw_title.lower():
            title_parts.append(color)

        final_title = ", ".join(title_parts)

        marketing_tags = []
        if raw_data.get("new"):
            marketing_tags.append("Новинка")
        if raw_data.get("recomended"):
            marketing_tags.append("Рекомендуем")

        for action in raw_data.get("action_labels", []):
            if isinstance(action, str):
                marketing_tags.append(action)
            elif isinstance(action, dict) and action.get("name"):
                marketing_tags.append(action["name"])

        section = []
        category = raw_data.get("category", {})
        parent_category = category.get("parent", {})

        if parent_category and parent_category.get("name"):
            section.append(parent_category.get("name"))
        if category and category.get("name"):
            section.append(category.get("name"))

        sale_tag = ""
        current_price = float(raw_data.get("price") or 0.0)
        prev_price = raw_data.get("prev_price")
        original_price = float(prev_price) if prev_price else current_price
        if original_price > current_price:
            discount = 100 - (current_price / original_price * 100)
            sale_tag = f"Скидка {int(discount)}%"

        return {
            "timestamp": int(time.time()),
            "RPC": str(raw_data.get("vendor_code", raw_data.get("uuid", ""))),
            "url": raw_data.get("product_url", ""),
            "title": final_title,
            "marketing_tags": marketing_tags,
            "brand": "",
            "section": section,
            "price_data": {
                "current": current_price,
                "original": original_price,
                "sale_tag": sale_tag
            },
            "stock": {
                "in_stock": raw_data.get("available", False),
                "count": raw_data.get("quantity_total", 0)
            },
            "assets": {
                "main_image": raw_data.get("image_url", ""),
                "set_images": [],
                "view360": [],
                "video": []
            },
            "metadata": metadata,
            "variants": 1
        }
