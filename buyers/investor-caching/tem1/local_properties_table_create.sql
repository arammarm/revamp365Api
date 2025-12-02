-- Table: investor_cache.properties

-- DROP TABLE IF EXISTS investor_cache.properties;

CREATE TABLE IF NOT EXISTS investor_cache.properties
(
    property_id bigint NOT NULL DEFAULT nextval('investor_cache.properties_property_id_seq'::regclass),
    investor_id bigint NOT NULL,
    datatree_id bigint NOT NULL,
    apn character varying(50) COLLATE pg_catalog."default",
    fips character varying(10) COLLATE pg_catalog."default" NOT NULL,
    situs_address character varying(255) COLLATE pg_catalog."default",
    situs_city character varying(100) COLLATE pg_catalog."default",
    situs_state character(2) COLLATE pg_catalog."default",
    situs_zip5 character varying(10) COLLATE pg_catalog."default",
    location geography(Point,4326),
    property_class character varying(10) COLLATE pg_catalog."default",
    bedrooms smallint,
    bathrooms numeric(5,1),
    sqft integer,
    year_built smallint,
    purchase_date date,
    purchase_price numeric(12,2),
    cash_purchase boolean DEFAULT false,
    sale_date date,
    sale_price numeric(12,2),
    currently_owned boolean DEFAULT true,
    is_absentee boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    activity_type character varying COLLATE pg_catalog."default",
    entity_name character varying COLLATE pg_catalog."default",
    style_code integer,
    property_type character varying COLLATE pg_catalog."default",
    concurrent_mtg1_loan_amt integer,
    concurrent_mtg2_loan_amt integer,
    lot_size integer,
    arv numeric(10,2),
    avm numeric(10,2),
    accuracy_score numeric(3,2),
    calculated boolean DEFAULT false,
    CONSTRAINT properties_pkey PRIMARY KEY (property_id),
    CONSTRAINT properties_datatree_investor_unique UNIQUE (datatree_id, investor_id),
    CONSTRAINT properties_investor_id_fkey FOREIGN KEY (investor_id)
        REFERENCES investor_cache.investors (investor_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS investor_cache.properties
    OWNER to doadmin;

COMMENT ON COLUMN investor_cache.properties.sale_date
    IS 'Date when the property was sold by this investor';

COMMENT ON COLUMN investor_cache.properties.sale_price
    IS 'Price at which the property was sold';
-- Index: idx_prop_datatree

-- DROP INDEX IF EXISTS investor_cache.idx_prop_datatree;

CREATE INDEX IF NOT EXISTS idx_prop_datatree
    ON investor_cache.properties USING btree
    (datatree_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_prop_fips

-- DROP INDEX IF EXISTS investor_cache.idx_prop_fips;

CREATE INDEX IF NOT EXISTS idx_prop_fips
    ON investor_cache.properties USING btree
    (fips COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_prop_investor

-- DROP INDEX IF EXISTS investor_cache.idx_prop_investor;

CREATE INDEX IF NOT EXISTS idx_prop_investor
    ON investor_cache.properties USING btree
    (investor_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_prop_location

-- DROP INDEX IF EXISTS investor_cache.idx_prop_location;

CREATE INDEX IF NOT EXISTS idx_prop_location
    ON investor_cache.properties USING gist
    (location)
    TABLESPACE pg_default;
-- Index: idx_prop_purchase_date

-- DROP INDEX IF EXISTS investor_cache.idx_prop_purchase_date;

CREATE INDEX IF NOT EXISTS idx_prop_purchase_date
    ON investor_cache.properties USING btree
    (purchase_date DESC NULLS FIRST)
    TABLESPACE pg_default;
-- Index: idx_prop_zip

-- DROP INDEX IF EXISTS investor_cache.idx_prop_zip;

CREATE INDEX IF NOT EXISTS idx_prop_zip
    ON investor_cache.properties USING btree
    (situs_zip5 COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_properties_currently_owned

-- DROP INDEX IF EXISTS investor_cache.idx_properties_currently_owned;

CREATE INDEX IF NOT EXISTS idx_properties_currently_owned
    ON investor_cache.properties USING btree
    (currently_owned ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE currently_owned = true;
-- Index: idx_properties_fips_date

-- DROP INDEX IF EXISTS investor_cache.idx_properties_fips_date;

CREATE INDEX IF NOT EXISTS idx_properties_fips_date
    ON investor_cache.properties USING btree
    (fips COLLATE pg_catalog."default" ASC NULLS LAST, purchase_date DESC NULLS FIRST)
    TABLESPACE pg_default;
-- Index: idx_properties_sale_date

-- DROP INDEX IF EXISTS investor_cache.idx_properties_sale_date;

CREATE INDEX IF NOT EXISTS idx_properties_sale_date
    ON investor_cache.properties USING btree
    (sale_date ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE sale_date IS NOT NULL;
-- Index: idx_properties_spatial

-- DROP INDEX IF EXISTS investor_cache.idx_properties_spatial;

CREATE INDEX IF NOT EXISTS idx_properties_spatial
    ON investor_cache.properties USING gist
    (location)
    TABLESPACE pg_default;
-- Index: unique_properties_datatree_investor

-- DROP INDEX IF EXISTS investor_cache.unique_properties_datatree_investor;

CREATE UNIQUE INDEX IF NOT EXISTS unique_properties_datatree_investor
    ON investor_cache.properties USING btree
    (datatree_id ASC NULLS LAST, investor_id ASC NULLS LAST)
    TABLESPACE pg_default;