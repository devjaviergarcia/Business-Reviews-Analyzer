export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = this.normalizeBaseUrl(baseUrl);
  }

  public getBaseUrl(): string {
    return this.baseUrl;
  }

  public setBaseUrl(value: string): void {
    this.baseUrl = this.normalizeBaseUrl(value);
  }

  public async get<T>(path: string): Promise<T> {
    const response = await fetch(this.buildUrl(path));
    if (!response.ok) {
      throw new Error(await response.text());
    }
    return (await response.json()) as T;
  }

  public async post<T>(path: string, payload: Record<string, unknown>): Promise<T> {
    const response = await fetch(this.buildUrl(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(await response.text());
    }
    return (await response.json()) as T;
  }

  public async delete<T>(path: string): Promise<T> {
    const response = await fetch(this.buildUrl(path), {
      method: "DELETE",
    });
    if (!response.ok) {
      throw new Error(await response.text());
    }
    return (await response.json()) as T;
  }

  public createEventSource(path: string): EventSource {
    return new EventSource(this.buildUrl(path));
  }

  private buildUrl(path: string): string {
    return `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
  }

  private normalizeBaseUrl(raw: string): string {
    const value = (raw || "http://localhost:8000").trim();
    return value.replace(/\/+$/, "");
  }
}
