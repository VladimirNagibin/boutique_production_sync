# Создание таблиц
sql_script_create_table = """
-- Таблица для кодов товаров поставщиков
CREATE TABLE IF NOT EXISTS supplier_product_codes (
    -- Первичный ключ (автоинкремент)
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Код товара у поставщика
    code INTEGER NOT NULL CHECK (code > 0),

    -- Наименование товара поставщика
    name TEXT NOT NULL CHECK (LENGTH(TRIM(name)) > 0),

    -- Группа товаров
    product_group TEXT,

    -- Подгруппа товаров
    subgroup TEXT,

    -- Идентификатор поставщика
    supplier_id INTEGER NOT NULL,

    -- Уникальность связки (код поставщика + код товара)
    CONSTRAINT unique_supplier_code UNIQUE (code, supplier_id)
);

-- Индекс для быстрого поиска по поставщику и коду
CREATE INDEX IF NOT EXISTS idx_supplier_code
ON supplier_product_codes (supplier_id, code);

-- Индекс для поиска по названию товара
CREATE INDEX IF NOT EXISTS idx_product_name
ON supplier_product_codes (name);

"""
