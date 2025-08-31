

WITH

source AS (
  SELECT 
    ip,
    hostname,
    city,
    region,
    country,
    loc,
    org,
    postal,
    timezone,
    asn,
    company,
    carrier,
    is_bogon,
    is_mobile,
    multiIf(
      lowerUTF8(org) ILIKE '%amazon web services%' OR lowerUTF8(org) ILIKE '%amazon data services%' OR lowerUTF8(org) ILIKE '%aws%' OR lowerUTF8(org) ILIKE '%amazon.com%', 'AWS',
      (lowerUTF8(org) ILIKE '%google cloud%' OR lowerUTF8(org) ILIKE '%google llc%' OR lowerUTF8(org) ILIKE '%gcp%' OR lowerUTF8(org) ILIKE '%google%') AND lowerUTF8(org) NOT ILIKE '%fiber%', 'Google',
      lowerUTF8(org) ILIKE '%microsoft azure%' OR lowerUTF8(org) ILIKE '%azure%' OR lowerUTF8(org) ILIKE '%microsoft corporation%' OR lowerUTF8(org) ILIKE '%msft%', 'Azure',
      lowerUTF8(org) ILIKE '%oracle cloud%' OR lowerUTF8(org) ILIKE '%oracle america%' OR lowerUTF8(org) ILIKE '%oci%', 'Oracle Cloud',
      lowerUTF8(org) ILIKE '%alibaba cloud%' OR lowerUTF8(org) ILIKE '%aliyun%' OR lowerUTF8(org) ILIKE '%alibaba%', 'Alibaba Cloud',

      lowerUTF8(org) ILIKE '%cloudflare%', 'Cloudflare',
      (lowerUTF8(org) ILIKE '%akamai%' AND lowerUTF8(org) NOT ILIKE '%linode%'), 'Akamai',
      lowerUTF8(org) ILIKE '%fastly%', 'Fastly',

      lowerUTF8(org) ILIKE '%digitalocean%' OR lowerUTF8(org) ILIKE '%digital ocean%', 'DigitalOcean',
      lowerUTF8(org) ILIKE '%ovh%', 'OVHcloud',
      lowerUTF8(org) ILIKE '%hetzner%', 'Hetzner',
      lowerUTF8(org) ILIKE '%scaleway%' OR lowerUTF8(org) ILIKE '%online s.a.s%' OR lowerUTF8(org) ILIKE '%iliad%', 'Scaleway',
      lowerUTF8(org) ILIKE '%linode%', 'Linode',
      lowerUTF8(org) ILIKE '%vultr%' OR lowerUTF8(org) ILIKE '%choopa%', 'Vultr',
      lowerUTF8(org) ILIKE '%equinix metal%' OR lowerUTF8(org) ILIKE '%packet host%' OR lowerUTF8(org) ILIKE '%packet, inc%', 'Equinix Metal',
      lowerUTF8(org) ILIKE '%fly.io%' OR lowerUTF8(org) ILIKE '%fly io%', 'Fly.io',
      lowerUTF8(org) ILIKE '%netlify%', 'Netlify',
      lowerUTF8(org) ILIKE '%vercel%' OR lowerUTF8(org) ILIKE '%zeit%', 'Vercel',
      lowerUTF8(org) ILIKE '%heroku%', 'Heroku',
      lowerUTF8(org) ILIKE '%render.com%' OR lowerUTF8(org) ILIKE '% render %', 'Render',

      -- Carrier / transit networks (keep as its own bucket; change to 'Public ISP (Home/Office)' if you prefer)
      lowerUTF8(org) ILIKE '%cogent%' OR lowerUTF8(org) ILIKE '%lumen%' OR lowerUTF8(org) ILIKE '%level 3%' OR lowerUTF8(org) ILIKE '%centurylink%' OR
      lowerUTF8(org) ILIKE '%telia carrier%' OR lowerUTF8(org) ILIKE '%arelion%' OR lowerUTF8(org) ILIKE '%gtt%' OR lowerUTF8(org) ILIKE '%hurricane electric%' OR
      lowerUTF8(org) ILIKE '%he.net%' OR lowerUTF8(org) ILIKE '%ntt communications%' OR lowerUTF8(org) ILIKE '%tata communications%' OR lowerUTF8(org) ILIKE '%zayo%' OR
      lowerUTF8(org) ILIKE '%kddi%' OR lowerUTF8(org) ILIKE '%sparkle%' OR lowerUTF8(org) ILIKE '%backbone%' OR lowerUTF8(org) ILIKE '%chinanet-backbone%', 'Carrier/Transit',

      -- Education / government → treat like public access
      lowerUTF8(org) ILIKE '%university%' OR lowerUTF8(org) ILIKE '%college%' OR lowerUTF8(org) ILIKE '%school%' OR lowerUTF8(org) ILIKE '%ministry%' OR lowerUTF8(org) ILIKE '%government%', 'Public ISP (Home/Office)',

      -- Generic hosting/CDN hints
      lowerUTF8(org) ILIKE '%datacenter%' OR lowerUTF8(org) ILIKE '%data center%' OR lowerUTF8(org) ILIKE '%colo%' OR lowerUTF8(org) ILIKE '%hosting%' OR lowerUTF8(org) ILIKE '%vps%' OR lowerUTF8(org) ILIKE '%server%' OR lowerUTF8(org) ILIKE '%incapsula%' OR lowerUTF8(org) ILIKE '%imperva%', 'Hosting/CDN (Other)',

      -- Fallback for residential & office ISPs
      lowerUTF8(org) ILIKE '%telecom%' OR lowerUTF8(org) ILIKE '%telekom%' OR lowerUTF8(org) ILIKE '%telecommunications%' OR lowerUTF8(org) ILIKE '%communications%' OR lowerUTF8(org) ILIKE '%wireless%' OR
      lowerUTF8(org) ILIKE '%cable%' OR lowerUTF8(org) ILIKE '%broadband%' OR lowerUTF8(org) ILIKE '%internet%' OR lowerUTF8(org) ILIKE '%fibre%' OR lowerUTF8(org) ILIKE '%fiber%' OR lowerUTF8(org) ILIKE '%mobile%', 'Public ISP (Home/Office)',
      lowerUTF8(org) = '', 'Unknown',
      'Public ISP (Home/Office)'
  ) AS generic_provider
  FROM `crawlers_data`.`ipinfo` 
)

SELECT * FROM source