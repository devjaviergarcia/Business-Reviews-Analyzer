from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Any


def compute_reputation_score(
    *,
    avg_rating: float,
    response_rate: float,
    negative_ratio: float,
    sentiment_avg: float,
    tranquility_avg: float,
    clamp,
    clamp01,
) -> float:
    rating_component = clamp01(avg_rating / 5.0) * 55.0
    response_component = clamp01(response_rate) * 20.0
    sentiment_component = clamp01((sentiment_avg + 1.0) / 2.0) * 15.0
    stability_component = clamp01((tranquility_avg + 1.0) / 2.0) * 10.0
    penalty = clamp01(negative_ratio) * 10.0
    score = rating_component + response_component + sentiment_component + stability_component - penalty
    return round(clamp(score, 0.0, 100.0), 2)


def negative_ratio(*, review_metrics: list[dict[str, Any]], safe_float) -> float:
    if not review_metrics:
        return 0.0
    negative_count = 0
    for item in review_metrics:
        rating = safe_float(item.get("rating"))
        sentiment = safe_float((item.get("dimensions") or {}).get("sentiment"))
        if rating <= 2.0 or sentiment <= -0.25:
            negative_count += 1
    return negative_count / max(1, len(review_metrics))


def average_dimension(
    review_metrics: list[dict[str, Any]],
    key: str,
    *,
    safe_float,
) -> float:
    if not review_metrics:
        return 0.0
    values: list[float] = []
    for item in review_metrics:
        dims = item.get("dimensions") if isinstance(item.get("dimensions"), dict) else {}
        values.append(safe_float(dims.get(key)))
    return statistics.mean(values) if values else 0.0


def label_customer_cluster(
    centroid: list[float],
    *,
    cluster_id: int | None,
    used_labels: set[str] | None,
) -> tuple[str, str]:
    sentiment, expectation, satisfaction, tranquility, improvement, _ = centroid

    candidates: list[tuple[str, str]] = []
    if satisfaction >= 0.8 and sentiment >= 0.4 and tranquility >= 0.55 and expectation < 0.15:
        candidates.append(
            (
                "Promotores fieles",
                "Clientes muy satisfechos y con alta probabilidad de repetir y recomendar.",
            )
        )
    if satisfaction >= 0.68 and sentiment >= 0.25:
        candidates.append(
            (
                "Promotores satisfechos",
                "Valoran positivamente la experiencia y pueden convertirse en embajadores de marca.",
            )
        )
    if sentiment <= -0.1 and tranquility < -0.05:
        candidates.append(
            (
                "Críticos intensos",
                "Clientes con fricción emocional alta que requieren recuperación prioritaria.",
            )
        )
    if expectation >= 0.38 and improvement >= 0.42:
        candidates.append(
            (
                "Exigentes constructivos",
                "Ven margen de mejora y aportan señales útiles para rediseñar servicio.",
            )
        )
    candidates.append(
        (
            "Neutrales pragmáticos",
            "Segmento estable con satisfacción media y sensibilidad a mejoras operativas.",
        )
    )

    labels_in_use = used_labels if isinstance(used_labels, set) else None
    for label, description in candidates:
        if labels_in_use is None or label not in labels_in_use:
            if labels_in_use is not None:
                labels_in_use.add(label)
            return label, description

    base_label, base_description = candidates[0]
    if cluster_id is None:
        cluster_id = 0
    unique_label = f"{base_label} ({cluster_id + 1})"
    if labels_in_use is not None:
        labels_in_use.add(unique_label)
    return unique_label, base_description


def theme_scores(
    text_norm: str,
    *,
    theme_keywords: dict[str, tuple[str, ...]],
    count_keyword_hits,
) -> dict[str, int]:
    if not text_norm:
        return {}
    scores: dict[str, int] = {}
    for theme, keywords in theme_keywords.items():
        hits = count_keyword_hits(text_norm, keywords)
        if hits > 0:
            scores[theme] = hits
    return scores


def resolve_dominant_problem(
    *,
    rating: float,
    text_norm: str,
    sentiment: float,
    theme_scores: dict[str, int],
    generic_comment_problem: str,
    positive_comment_problem: str,
    negative_comment_problem: str,
    no_comment_high_problem: str,
    no_comment_medium_problem: str,
    no_comment_low_problem: str,
) -> str:
    if theme_scores:
        return max(theme_scores.items(), key=lambda item: item[1])[0]

    if not text_norm:
        if rating >= 4.0:
            return no_comment_high_problem
        if rating <= 2.0:
            return no_comment_low_problem
        return no_comment_medium_problem

    if rating <= 2.5 or sentiment <= -0.2:
        return negative_comment_problem
    if rating >= 4.0 or sentiment >= 0.25:
        return positive_comment_problem
    return generic_comment_problem


def safe_rating(value: Any, *, clamp) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = 0.0
    return clamp(parsed, 0.0, 5.0)


def safe_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def upper_ratio(text: str) -> float:
    letters = [ch for ch in str(text or "") if ch.isalpha()]
    if not letters:
        return 0.0
    uppercase = [ch for ch in letters if ch.isupper()]
    return len(uppercase) / len(letters)


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def clamp01(value: float) -> float:
    return clamp(value, 0.0, 1.0)


def linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    n = len(values)
    x_mean = (n - 1) / 2.0
    y_mean = statistics.mean(values)
    numerator = 0.0
    denominator = 0.0
    for idx, value in enumerate(values):
        dx = idx - x_mean
        numerator += dx * (value - y_mean)
        denominator += dx * dx
    if denominator == 0:
        return 0.0
    return numerator / denominator


def kmeans(*, features: list[list[float]], k: int, max_iter: int) -> tuple[list[int], list[list[float]]]:
    if not features:
        return [], []
    k = max(1, min(k, len(features)))
    if k == 1:
        centroid = [statistics.mean(values) for values in zip(*features)]
        return [0 for _ in features], [centroid]

    normalized_features, means, stds = zscore(features)

    initial_indexes = [0]
    while len(initial_indexes) < k:
        farthest_index = 0
        farthest_distance = -1.0
        for idx, point in enumerate(normalized_features):
            if idx in initial_indexes:
                continue
            distance = min(
                math.dist(point, normalized_features[center_idx]) for center_idx in initial_indexes
            )
            if distance > farthest_distance:
                farthest_distance = distance
                farthest_index = idx
        initial_indexes.append(farthest_index)

    centroids = [list(normalized_features[idx]) for idx in initial_indexes]
    labels = [0 for _ in normalized_features]

    for _ in range(max(1, max_iter)):
        changed = False
        for idx, point in enumerate(normalized_features):
            best_cluster = 0
            best_distance = float("inf")
            for cluster_id, centroid in enumerate(centroids):
                distance = math.dist(point, centroid)
                if distance < best_distance:
                    best_distance = distance
                    best_cluster = cluster_id
            if labels[idx] != best_cluster:
                labels[idx] = best_cluster
                changed = True

        cluster_points: dict[int, list[list[float]]] = defaultdict(list)
        for idx, label in enumerate(labels):
            cluster_points[label].append(normalized_features[idx])

        for cluster_id in range(k):
            points = cluster_points.get(cluster_id) or []
            if not points:
                continue
            centroids[cluster_id] = [statistics.mean(values) for values in zip(*points)]

        if not changed:
            break

    denormalized_centroids: list[list[float]] = []
    for centroid in centroids:
        denormalized = []
        for idx, value in enumerate(centroid):
            denormalized.append((value * stds[idx]) + means[idx])
        denormalized_centroids.append(denormalized)

    return labels, denormalized_centroids


def zscore(features: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
    dimensions = len(features[0])
    means: list[float] = []
    stds: list[float] = []
    for dim in range(dimensions):
        values = [row[dim] for row in features]
        mean_value = statistics.mean(values) if values else 0.0
        std_value = statistics.pstdev(values) if len(values) >= 2 else 0.0
        means.append(mean_value)
        stds.append(std_value if std_value > 1e-9 else 1.0)

    normalized: list[list[float]] = []
    for row in features:
        normalized.append([((row[idx] - means[idx]) / stds[idx]) for idx in range(dimensions)])
    return normalized, means, stds
