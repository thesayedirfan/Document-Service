CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    search_vector tsvector,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE FUNCTION documents_search_trigger() RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        to_tsvector(
            'english',
            coalesce(NEW.title,'') || ' ' ||
            coalesce(NEW.content,'')
        );
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER search_vector_update
BEFORE INSERT OR UPDATE
ON documents
FOR EACH ROW
EXECUTE FUNCTION documents_search_trigger();

CREATE INDEX idx_documents_search
ON documents
USING GIN(search_vector);

CREATE INDEX idx_documents_tenant
ON documents(tenant_id);