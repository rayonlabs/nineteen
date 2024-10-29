-- migrate:up
CREATE TABLE IF NOT EXISTS random_text (
    id SERIAL PRIMARY KEY,
    text TEXT NOT NULL,
    n_words INTEGER NOT NULL,
    n_paragraphes INTEGER NOT NULL,
    n_sentences INTEGER NOT NULL
);

-- migrate:down
DROP TABLE IF EXISTS random_text;
