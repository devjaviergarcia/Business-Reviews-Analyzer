export type MenuKey = "analysis" | "jobs" | "business" | "api";

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
  name?: string;
  strategy?: string;
  created_at?: string;
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
