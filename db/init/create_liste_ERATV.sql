CREATE TABLE IF NOT EXISTS liste_ERATV (
    Type_ID TEXT PRIMARY KEY,
    Authorisation_document_reference_EIN TEXT,
    Type_Name TEXT,
    Authorisation_Status TEXT,
    Last_Update DATE,
    URL_TV TEXT NOT NULL
);
