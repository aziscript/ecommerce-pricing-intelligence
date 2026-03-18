"""
catalog.py
Shared product catalog imported by all generators.
30 products across 5 categories, prices $10–$2000.
"""

PRODUCTS: list[dict] = [
    # Phones (6)
    {"product_id": "P001", "product_name": "Samsung Galaxy S24",             "product_category": "phones",      "product_price": 899.99},
    {"product_id": "P002", "product_name": "iPhone 15 Pro",                  "product_category": "phones",      "product_price": 1199.99},
    {"product_id": "P003", "product_name": "Google Pixel 8",                 "product_category": "phones",      "product_price": 699.99},
    {"product_id": "P004", "product_name": "OnePlus 12",                     "product_category": "phones",      "product_price": 799.99},
    {"product_id": "P005", "product_name": "Xiaomi 14 Pro",                  "product_category": "phones",      "product_price": 649.99},
    {"product_id": "P006", "product_name": "Motorola Edge 40",               "product_category": "phones",      "product_price": 349.99},
    # Laptops (6)
    {"product_id": "P007", "product_name": "MacBook Pro 16-inch",            "product_category": "laptops",     "product_price": 1999.99},
    {"product_id": "P008", "product_name": "Dell XPS 15",                    "product_category": "laptops",     "product_price": 1799.99},
    {"product_id": "P009", "product_name": "Lenovo ThinkPad X1 Carbon",      "product_category": "laptops",     "product_price": 1599.99},
    {"product_id": "P010", "product_name": "ASUS ROG Zephyrus G14",          "product_category": "laptops",     "product_price": 1399.99},
    {"product_id": "P011", "product_name": "HP Spectre x360",                "product_category": "laptops",     "product_price": 1299.99},
    {"product_id": "P012", "product_name": "Acer Swift 3",                   "product_category": "laptops",     "product_price": 649.99},
    # Headphones (6)
    {"product_id": "P013", "product_name": "Sony WH-1000XM5",               "product_category": "headphones",  "product_price": 349.99},
    {"product_id": "P014", "product_name": "Apple AirPods Pro 2",            "product_category": "headphones",  "product_price": 249.99},
    {"product_id": "P015", "product_name": "Bose QuietComfort 45",           "product_category": "headphones",  "product_price": 329.99},
    {"product_id": "P016", "product_name": "Sennheiser Momentum 4",          "product_category": "headphones",  "product_price": 299.99},
    {"product_id": "P017", "product_name": "Jabra Evolve2 85",               "product_category": "headphones",  "product_price": 379.99},
    {"product_id": "P018", "product_name": "Anker Soundcore Q45",            "product_category": "headphones",  "product_price": 59.99},
    # Tablets (6)
    {"product_id": "P019", "product_name": "iPad Pro 12.9-inch",             "product_category": "tablets",     "product_price": 1099.99},
    {"product_id": "P020", "product_name": "Samsung Galaxy Tab S9",          "product_category": "tablets",     "product_price": 799.99},
    {"product_id": "P021", "product_name": "Microsoft Surface Pro 9",        "product_category": "tablets",     "product_price": 1299.99},
    {"product_id": "P022", "product_name": "Amazon Fire HD 10",              "product_category": "tablets",     "product_price": 149.99},
    {"product_id": "P023", "product_name": "Lenovo Tab P12 Pro",             "product_category": "tablets",     "product_price": 549.99},
    {"product_id": "P024", "product_name": "Xiaomi Pad 6 Pro",               "product_category": "tablets",     "product_price": 399.99},
    # Accessories (6)
    {"product_id": "P025", "product_name": "Anker USB-C Hub 7-in-1",         "product_category": "accessories", "product_price": 49.99},
    {"product_id": "P026", "product_name": "Samsung 65W USB-C Charger",      "product_category": "accessories", "product_price": 39.99},
    {"product_id": "P027", "product_name": "Apple MagSafe Charger",          "product_category": "accessories", "product_price": 39.99},
    {"product_id": "P028", "product_name": "Logitech MX Master 3S",          "product_category": "accessories", "product_price": 99.99},
    {"product_id": "P029", "product_name": "SanDisk 1TB Portable SSD",       "product_category": "accessories", "product_price": 129.99},
    {"product_id": "P030", "product_name": "Belkin 3-in-1 Wireless Charger", "product_category": "accessories", "product_price": 79.99},
]

# Convenience lookup by product_id
PRODUCT_BY_ID: dict[str, dict] = {p["product_id"]: p for p in PRODUCTS}
