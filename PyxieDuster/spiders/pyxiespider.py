from urllib.parse import urlencode
import scrapy
from PyxieDuster.items import PyxieItem

# Api key from scrapeops.io
API_KEY = ""


def get_proxy_url(url):
    payload = {"api_key": API_KEY, "url": url}
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    return proxy_url


class PyxiespiderSpider(scrapy.Spider):
    name = "pyxiespider"
    allowed_domains = ["books.toscrape.com", "proxy.scrapeops.io"]
    start_urls = ["https://books.toscrape.com"]

    custom_settings = {
        "FEEDS": {
            "data.json": {"format": "json", "overwrite": True},
        }
    }

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse)

    def parse(self, response):
        books = response.css("article.product_pod")

        # Loop through books on each page
        for book in books:
            relative_url = book.css("h3 a ::attr(href)").get()

            if "catalogue/" in relative_url:
                book_url = "https://books.toscrape.com/" + relative_url
            else:
                book_url = "https://books.toscrape.com/catalogue/" + relative_url

            yield response.follow(
                url=book_url,
                callback=self.parse_book_page,
            )

        # Scrape through pages until the last page
        next_page = response.css("li.next a ::attr(href)").get()

        if next_page is not None:
            if "catalogue/" in next_page:
                next_page_url = "https://books.toscrape.com/" + next_page
            else:
                next_page_url = "https://books.toscrape.com/catalogue/" + next_page
            yield response.follow(
                url=next_page_url,
                callback=self.parse,
            )

    def parse_book_page(self, response):
        table_rows = response.css("table tr")
        pyxie_item = PyxieItem()

        # Scrape data of each book page

        pyxie_item["url"] = (response.url,)
        pyxie_item["title"] = (response.css(".product_main h1::text").get(),)
        pyxie_item["upc"] = (table_rows[0].css("td ::text").get(),)
        pyxie_item["product_type"] = (table_rows[1].css("td ::text").get(),)
        pyxie_item["price_excl_tax"] = (table_rows[2].css("td ::text").get(),)
        pyxie_item["price_incl_tax"] = (table_rows[3].css("td ::text").get(),)
        pyxie_item["tax"] = (table_rows[4].css("td ::text").get(),)
        pyxie_item["availability"] = (table_rows[5].css("td ::text").get(),)
        pyxie_item["num_of_reviews"] = (table_rows[6].css("td ::text").get(),)
        pyxie_item["stars"] = (response.css("p.star-rating").attrib["class"],)
        pyxie_item["category"] = (
            response.xpath(
                "//ul[@class='breadcrumb']/li[@class='active']/preceding-sibling::li[1]/a/text()"
            ).get(),
        )
        pyxie_item["description"] = (
            response.xpath(
                "//div[@id='product_description']/following-sibling::p/text()"
            ).get(),
        )
        pyxie_item["price"] = (response.css("p.price_color ::text").get(),)

        yield pyxie_item
