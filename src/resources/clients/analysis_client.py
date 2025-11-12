from httpx import Client, HTTPStatusError


class AnalysisClient:
    def __init__(self, analysis_nginx_client_base_url: str) -> None:
        self.analysis_nginx_client_base_url = analysis_nginx_client_base_url
        self.client = Client(base_url=f"http://{self.analysis_nginx_client_base_url}/analysis",
                             follow_redirects=True)

    def inform_analysis(self, analysis_id: str, result: dict) -> dict:
        response = self.client.post(f"/{analysis_id}/nextflow",
                                    json=result,
                                    headers={"Content-Type": "application/json"})
        try:
            response.raise_for_status()
        except HTTPStatusError as e:
            print("HTTP Error in analysis client:", repr(e))

        return response.json()
