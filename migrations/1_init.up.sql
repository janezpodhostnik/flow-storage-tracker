CREATE TABLE storage
(
    address      CHAR(18)  NOT NULL PRIMARY KEY,
    storage_used BIGINT    NOT NULL,
    block_height BIGINT    NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE storage_history
(
    id           SERIAL PRIMARY KEY,
    address      CHAR(18)  NOT NULL,
    storage_used BIGINT    NOT NULL,
    block_height BIGINT    NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);

CREATE INDEX storage_history_address_idx ON storage_history (address);

CREATE OR REPLACE FUNCTION storage_before_update_trigger() RETURNS TRIGGER AS
$$
BEGIN
    -- if the block height is older ignore the update
    IF NEW.block_height <= OLD.block_height THEN
        RETURN NULL;
    END IF;
    -- if the value is the same ignore the update
    IF NEW.address = OLD.address AND
       NEW.storage_used = OLD.storage_used THEN
        RETURN NULL;
    END IF;
    INSERT INTO storage_history (address, storage_used, block_height, updated_at)
    VALUES (OLD.address, OLD.storage_used, OLD.block_height, OLD.updated_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER storage_before_update
    BEFORE UPDATE
    ON storage
    FOR EACH ROW
EXECUTE FUNCTION storage_before_update_trigger();
