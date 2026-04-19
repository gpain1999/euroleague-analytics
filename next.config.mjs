/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,

  // DuckDB utilise des bindings natifs Node.js : il faut indiquer à Next.js
  // de ne pas les bundler pour qu'ils soient résolus à l'exécution côté serveur.
  experimental: {
    serverComponentsExternalPackages: ['@duckdb/node-api']
  },

  // Images distantes autorisées (logos, photos joueurs récupérés par le pipeline).
  // Pour l'instant on privilégie l'hébergement local dans /public/images.
  images: {
    remotePatterns: [
      { protocol: 'https', hostname: 'media-cdn.incrowdsports.com' },
      { protocol: 'https', hostname: 'media-cdn.cortextech.io' }
    ]
  }
};

export default nextConfig;
