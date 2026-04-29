from __future__ import annotations

import numpy as np

from hybrid_utils import DEFAULT_EMBEDDING_MODEL, load_embedding_model


class VectorEngineError(RuntimeError):
    pass


def ensure_faiss():
    try:
        import faiss
    except ImportError as exc:
        raise VectorEngineError(
            "Missing dependency 'faiss'. Install it with: python -m pip install faiss-cpu"
        ) from exc

    return faiss


def normalize_embeddings(embeddings: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return embeddings / norms


class VectorEngine:
    def __init__(self, model_name: str = DEFAULT_EMBEDDING_MODEL):
        self.model = load_embedding_model(model_name)
        self.faiss = ensure_faiss()
        self.index = self.faiss.IndexFlatIP(self.model.get_sentence_embedding_dimension())
        self.metadata_mapping: list[str] = []
        self.position_by_id: dict[str, int] = {}

    def rebuild_positions(self) -> None:
        self.position_by_id = {
            metadata_id: index for index, metadata_id in enumerate(self.metadata_mapping)
        }

    def encode_query(self, query_text: str) -> np.ndarray:
        query_embedding = self.model.encode([query_text], convert_to_numpy=True)
        return normalize_embeddings(query_embedding)[0]

    def add_documents(self, rows, batch_size: int = 64) -> None:
        metadata_ids = [row[0] for row in rows]
        texts = [row[1] for row in rows]
        embeddings = self.model.encode(texts, convert_to_numpy=True, batch_size=batch_size, show_progress_bar=False)
        self.index.add(normalize_embeddings(embeddings))
        self.metadata_mapping.extend(metadata_ids)
        self.rebuild_positions()

    def search(self, query_text: str, k: int = 5) -> list[tuple[str, float]]:
        distances, indices = self.index.search(self.encode_query(query_text)[None, :], k)

        results: list[tuple[str, float]] = []
        for i, idx in enumerate(indices[0]):
            if 0 <= idx < len(self.metadata_mapping):
                results.append((self.metadata_mapping[idx], float(distances[0][i])))
        return results

    def search_subset(self, query_text: str, metadata_ids: list[str], top_k: int = 5) -> list[tuple[str, float]]:
        query_vector = self.encode_query(query_text)
        results: list[tuple[str, float]] = []
        seen_ids: set[str] = set()

        for metadata_id in metadata_ids:
            if metadata_id in seen_ids:
                continue
            seen_ids.add(metadata_id)

            position = self.position_by_id.get(metadata_id)
            if position is None:
                continue

            vector = np.asarray(self.index.reconstruct(int(position)), dtype=np.float32)
            score = float(np.dot(vector, query_vector))
            results.append((metadata_id, score))

        results.sort(key=lambda item: item[1], reverse=True)
        return results[:top_k]

    def save(self, index_path, mapping_path) -> None:
        self.faiss.write_index(self.index, str(index_path))
        np.save(mapping_path, np.array(self.metadata_mapping, dtype=object))

    def load(self, index_path, mapping_path) -> None:
        self.index = self.faiss.read_index(str(index_path))
        self.metadata_mapping = np.load(mapping_path, allow_pickle=True).tolist()
        self.rebuild_positions()

    @classmethod
    def from_saved(
        cls,
        index_path,
        mapping_path,
        model_name: str = DEFAULT_EMBEDDING_MODEL,
    ):
        engine = cls(model_name)
        engine.load(index_path, mapping_path)
        return engine
