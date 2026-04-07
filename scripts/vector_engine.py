import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


class VectorEngine:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)
        self.index = faiss.IndexFlatIP(self.model.get_sentence_embedding_dimension())
        self.metadata_mapping = []

    def add_documents(self, rows):
        metadata_ids = [row[0] for row in rows]
        texts = [row[1] for row in rows]
        embeddings = self.model.encode(texts, convert_to_numpy=True, batch_size=64, show_progress_bar=True)
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        self.index.add(embeddings)
        self.metadata_mapping.extend(metadata_ids)

    def search(self, query_text, k=5):
        query_embedding = self.model.encode([query_text], convert_to_numpy=True)
        query_embedding = query_embedding / np.linalg.norm(query_embedding, axis=1, keepdims=True)
        distances, indices = self.index.search(query_embedding, k)

        results = []
        for i, idx in enumerate(indices[0]):
            if 0 <= idx < len(self.metadata_mapping):
                results.append((self.metadata_mapping[idx], float(distances[0][i])))
        return results
    
    def save(self, index_path, mapping_path):
        faiss.write_index(self.index, str(index_path))
        np.save(mapping_path, np.array(self.metadata_mapping))

    def load(self, index_path, mapping_path):
        self.index = faiss.read_index(str(index_path))
        self.metadata_mapping = np.load(mapping_path, allow_pickle=True).tolist()

    @classmethod
    def from_saved(cls, index_path, mapping_path, model_name='all-MiniLM-L6-v2'):
        engine = cls(model_name)
        engine.load(index_path, mapping_path)
        return engine
