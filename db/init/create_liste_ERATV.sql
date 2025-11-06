CREATE TABLE IF NOT EXISTS liste_ERATV (
    Type_ID VARCHAR(50) PRIMARY KEY,
    Authorisation_document_reference_EIN VARCHAR(100),
    Type_Name TEXT,
    Authorisation_Status VARCHAR(50),
    Last_Update VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    URL_TV TEXT NOT NULL
);

CREATE OR REPLACE FUNCTION update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_timestamp_on_liste_ERATV ON liste_ERATV;

CREATE TRIGGER set_timestamp_on_liste_ERATV
BEFORE UPDATE ON liste_ERATV
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_column();

CREATE INDEX IF NOT EXISTS idx_liste_ERATV_status ON liste_ERATV (Authorisation_Status);
CREATE INDEX IF NOT EXISTS idx_liste_ERATV_lastupdate ON liste_ERATV (Last_Update);
