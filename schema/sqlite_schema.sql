PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS products (
    parent_asin TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    main_category TEXT,
    title TEXT,
    average_rating REAL,
    rating_number INTEGER,
    price_text TEXT,
    price_value REAL,
    store TEXT,
    features_json TEXT NOT NULL,
    description_json TEXT NOT NULL,
    images_json TEXT NOT NULL,
    videos_json TEXT NOT NULL,
    categories_json TEXT NOT NULL,
    details_json TEXT NOT NULL,
    bought_together TEXT,
    search_text TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    asin TEXT,
    parent_asin TEXT NOT NULL,
    user_id TEXT NOT NULL,
    rating REAL NOT NULL,
    review_title TEXT,
    review_text TEXT,
    review_images_json TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    reviewed_at TEXT NOT NULL,
    helpful_vote INTEGER NOT NULL DEFAULT 0,
    verified_purchase INTEGER NOT NULL CHECK (verified_purchase IN (0, 1)),
    raw_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_category
    ON products(category);

CREATE INDEX IF NOT EXISTS idx_products_main_category
    ON products(main_category);

CREATE INDEX IF NOT EXISTS idx_products_average_rating
    ON products(average_rating);

CREATE INDEX IF NOT EXISTS idx_reviews_parent_asin
    ON reviews(parent_asin);

CREATE INDEX IF NOT EXISTS idx_reviews_user_id
    ON reviews(user_id);

CREATE INDEX IF NOT EXISTS idx_reviews_category
    ON reviews(category);

CREATE INDEX IF NOT EXISTS idx_reviews_timestamp_ms
    ON reviews(timestamp_ms);

CREATE INDEX IF NOT EXISTS idx_reviews_reviewed_at
    ON reviews(reviewed_at);
