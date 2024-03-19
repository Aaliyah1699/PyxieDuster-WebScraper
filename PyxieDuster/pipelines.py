import mysql.connector
# from dotenv import load_dotenv
# import os

# load_dotenv()

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class PyxiedusterPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        # Strip whitespace
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != "description":
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()

        # Price
        price_keys = ["price", "price_excl_tax", "price_incl_tax", "tax"]
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace("£", "")
            adapter[price_key] = float(value)

        # Extract number of books
        avail_string = adapter.get("availability")
        split_string = avail_string.split("(")
        if len(split_string) < 2:
            adapter["availability"] = 0
        else:
            avail_array = split_string[1].split(" ")
            adapter["availability"] = int(avail_array[0])

        # Convert reviews to int
        num_reviews = adapter.get("num_of_reviews")
        adapter["num_of_reviews"] = int(num_reviews)

        # Convert stars to int
        stars_string = adapter.get("stars")
        split_stars = stars_string.split(" ")
        stars_value = split_stars[1].lower()
        if stars_value == "zero":
            adapter["stars"] = 0
        elif stars_value == "one":
            adapter["stars"] = 1
        elif stars_value == "two":
            adapter["stars"] = 2
        elif stars_value == "three":
            adapter["stars"] = 3
        elif stars_value == "four":
            adapter["stars"] = 4
        elif stars_value == "five":
            adapter["stars"] = 5

        return item


class SaveToMySQLPipeline:

    def __init__(self):
        # * Add MySQL database: password(if set), and project db name
        self.conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="",
        )

        # Create cursor to execute commands
        self.cur = self.conn.cursor()

        # Create table if none exists
        self.cur.execute(
            """
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment, 
            url VARCHAR(255),
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_of_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text,
            PRIMARY KEY (id)
        )
        """
        )

    def process_item(self, item, spider):

        # Define insert statement
        self.cur.execute(
            """ insert into books (
            url, 
            title, 
            upc, 
            product_type, 
            price_excl_tax,
            price_incl_tax,
            tax,
            price,
            availability,
            num_of_reviews,
            stars,
            category,
            description
            ) values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )""",
            (
                item["url"],
                item["title"],
                item["upc"],
                item["product_type"],
                item["price_excl_tax"],
                item["price_incl_tax"],
                item["tax"],
                item["price"],
                item["availability"],
                item["num_of_reviews"],
                item["stars"],
                item["category"],
                str(item["description"][0]),
            ),
        )

        # Execute insert of data into database
        self.conn.commit()
        return item

    def close_spider(self, spider):

        # Close cursor & connection to database
        self.cur.close()
        self.conn.close()
