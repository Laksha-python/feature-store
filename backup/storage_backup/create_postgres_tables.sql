CREATE TABLE IF NOT EXISTS user_features (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_value DOUBLE PRECISION NOT NULL,
    feature_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, feature_name, feature_date)
);

CREATE INDEX IF NOT EXISTS idx_user_features_user_id ON user_features(user_id);
CREATE INDEX IF NOT EXISTS idx_user_features_feature_name ON user_features(feature_name);
CREATE INDEX IF NOT EXISTS idx_user_features_date ON user_features(feature_date);

CREATE TABLE IF NOT EXISTS product_features (
    id SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_value DOUBLE PRECISION NOT NULL,
    feature_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, feature_name, feature_date)
);

CREATE INDEX IF NOT EXISTS idx_product_features_product_id ON product_features(product_id);
CREATE INDEX IF NOT EXISTS idx_product_features_feature_name ON product_features(feature_name);
CREATE INDEX IF NOT EXISTS idx_product_features_date ON product_features(feature_date);

