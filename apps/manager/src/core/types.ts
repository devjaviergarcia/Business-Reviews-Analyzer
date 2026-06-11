export type MenuKey = "analysis" | "jobs" | "business" | "crm" | "studies" | "api";

export type JsonValue = string | number | boolean | null | JsonObject | JsonValue[];

export interface JsonObject {
  [key: string]: JsonValue;
}

export interface PaginatedResponse<T> {
  items?: T[];
  page?: number;
  page_size?: number;
  total?: number;
  total_pages?: number;
  has_next?: boolean;
  has_prev?: boolean;
}

export interface BusinessSummary {
  business_id: string;
  name: string;
  address?: string;
  overall_rating?: number;
  total_reviews?: number;
  description?: string;
  sources_available?: string[];
}

export interface BusinessSourceOverview {
  source: "google_maps" | "tripadvisor" | string;
  source_profile?: Record<string, unknown>;
  latest_job?: Record<string, unknown> | null;
  active_dataset?: Record<string, unknown> | null;
  comments_count?: number;
  latest_comments?: ReviewItem[];
}

export interface BusinessSourcesOverviewResponse {
  business_id: string;
  name?: string;
  name_normalized?: string;
  total_comments?: number;
  available_sources?: string[];
  source_counts?: Record<string, number>;
  sources?: BusinessSourceOverview[];
}

export interface ReviewItem {
  id?: string;
  author_name?: string;
  rating?: number;
  relative_time?: string;
  text?: string;
}

export interface AnalyzeJobItem {
  job_id: string;
  status: string;
  queue_name?: string;
  job_type?: string;
  source?: string;
  runtime_target?: string;
  execution_mode?: "automatic" | "live" | string;
  live_display_mode?: "native" | "xvfb" | string;
  requested_by?: string;
  fallback_policy?: string;
  human_session_id?: string | null;
  source_display_name?: string | null;
  attempts?: number;
  name?: string;
  name_normalized?: string;
  canonical_name?: string;
  canonical_name_normalized?: string;
  source_name?: string;
  source_name_normalized?: string;
  root_business_id?: string;
  strategy?: string;
  created_at?: string;
  started_at?: string;
  updated_at?: string;
  finished_at?: string;
  payload?: Record<string, unknown>;
  result?: Record<string, unknown>;
  error?: string;
  events?: JobEventItem[];
  progress?: {
    stage?: string;
    message?: string;
    status?: string;
  };
}

export interface JobEventItem {
  status?: string;
  stage?: string;
  message?: string;
  data?: Record<string, unknown>;
  created_at?: string;
}

export interface ViewModule {
  key: MenuKey;
  title: string;
  root: HTMLElement;
  onShow: () => void;
  onHide: () => void;
}

export interface CRMLeadItem {
  lead_id: string;
  business_name?: string;
  email?: string;
  phone?: string;
  website?: string;
  city?: string;
  category?: string;
  source?: string;
  status?: string;
  score?: number;
  legal?: {
    consent_status?: string;
    do_not_contact?: boolean;
    suppressed_reason?: string | null;
  };
  pipeline?: {
    business_id?: string | null;
    source_job_ids?: string[];
    analysis_job_id?: string | null;
    report_job_id?: string | null;
  };
  updated_at?: string;
  created_at?: string;
}

export interface CRMCampaignItem {
  campaign_id: string;
  name?: string;
  status?: string;
  source_mode?: string;
  selected_source?: string | null;
  cadence_template_id?: string | null;
  metrics?: Record<string, unknown>;
  launched_at?: string | null;
  created_at?: string;
  updated_at?: string;
}

export interface GeoPointItem {
  order: number;
  label: string;
  lat: number;
  lng: number;
}

export interface GeoCityItem {
  geo_city_id: string;
  city: string;
  city_slug: string;
  center: { lat: number; lng: number };
  points: GeoPointItem[];
  point_count: number;
  enabled?: boolean;
}

export interface GeoGridRunItem {
  geo_grid_run_id: string;
  keyword: string;
  city: string;
  city_slug: string;
  center?: { lat: number; lng: number };
  provider_mode?: "maps_live" | "uule" | string;
  grid_size?: number | null;
  grid_spacing_km?: number | null;
  uule_radius_m?: number | null;
  throttle_ms?: number | null;
  top_n: number;
  point_count: number;
  total_units: number;
  completed_units: number;
  completed_points: number;
  status: string;
  metrics?: Record<string, unknown>;
  job_id?: string | null;
  failure_reason?: string | null;
  created_at?: string;
  updated_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
}

export interface GeoGridResultItem {
  geo_grid_result_id: string;
  geo_grid_run_id: string;
  city_slug: string;
  keyword: string;
  point_order: number;
  point_label: string;
  grid_row?: number | null;
  grid_col?: number | null;
  lat: number;
  lng: number;
  rank: number;
  visible_top10?: boolean;
  provider_mode?: string | null;
  business_key: string;
  business_name: string;
  maps_url?: string | null;
  rating?: number | null;
  review_count?: number | null;
}

export interface GeoGridBusinessStats {
  business_key: string;
  business_name: string;
  maps_url?: string | null;
  rating?: number | null;
  review_count?: number | null;
  appearances: number;
  coverage_percent: number;
  missing_points: number;
  avg_rank?: number | null;
  best_rank?: number | null;
  worst_rank?: number | null;
  rank_stddev: number;
  top_1_count: number;
  top_3_count: number;
  top_5_count: number;
  top_10_count: number;
  top_20_count: number;
  points: Array<{ point_order: number; point_label?: string; lat: number; lng: number; rank: number }>;
}

export interface GeoGridPointStats {
  point_order: number;
  point_label?: string;
  grid_row?: number | null;
  grid_col?: number | null;
  lat: number;
  lng: number;
  top_results: Array<{
    rank: number;
    business_key: string;
    business_name: string;
    rating?: number | null;
    review_count?: number | null;
    maps_url?: string | null;
  }>;
}

export interface GeoGridStatsResponse {
  geo_grid_run_id: string;
  summary: Record<string, unknown> & {
    provider_mode?: string;
    visibility_score?: number | null;
    share_top3?: number | null;
    share_top10?: number | null;
    share_not_found?: number | null;
  };
  businesses: GeoGridBusinessStats[];
  leaders: GeoGridBusinessStats[];
  weakest: GeoGridBusinessStats[];
  most_consistent: GeoGridBusinessStats[];
  most_dispersed: GeoGridBusinessStats[];
  points: GeoGridPointStats[];
  run_metrics?: Record<string, unknown>;
}
