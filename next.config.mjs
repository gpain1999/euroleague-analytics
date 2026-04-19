/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,

  // DuckDB utilise des bindings natifs Node.js : indiquer à Next.js
  // de ne pas les bundler pour qu'ils soient résolus à l'exécution côté serveur.
  serverExternalPackages: ['@duckdb/node-api'],

  // Images distantes autorisées (logos, photos joueurs).
  images: {
    remotePatterns: [
      { protocol: 'https', hostname: 'media-cdn.incrowdsports.com' },
      { protocol: 'https', hostname: 'media-cdn.cortextech.io' }
    ]
  }
};

export default nextConfig;