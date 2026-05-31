import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      // VWAP+RSI Scalp FastAPI backend (port 8056) — REST + WebSocket
      {
        source: "/api/:path*",
        destination: "http://localhost:8056/:path*",
      },
    ];
  },
};

export default nextConfig;
