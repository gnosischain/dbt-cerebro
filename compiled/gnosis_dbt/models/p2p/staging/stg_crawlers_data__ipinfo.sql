

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
    -- Exposed so consumers can collapse the source's duplicate rows. ipinfo is a
    -- plain MergeTree ORDER BY (ip, updated_at), not Replacing, and ip_crawler
    -- appends a fresh row on every retry, so an IP can legitimately have several
    -- rows. Without a version column a downstream join on `ip` silently fans out.
    updated_at,
    multiIf(
      lowerUTF8(org) ILIKE '%amazon web services%' OR lowerUTF8(org) ILIKE '%amazon data services%' OR lowerUTF8(org) ILIKE '%aws%' OR lowerUTF8(org) ILIKE '%amazon.com%', 'AWS',
      (lowerUTF8(org) ILIKE '%google cloud%' OR lowerUTF8(org) ILIKE '%google llc%' OR lowerUTF8(org) ILIKE '%gcp%' OR lowerUTF8(org) ILIKE '%google%') AND lowerUTF8(org) NOT ILIKE '%fiber%', 'Google',
      lowerUTF8(org) ILIKE '%microsoft azure%' OR lowerUTF8(org) ILIKE '%azure%' OR lowerUTF8(org) ILIKE '%microsoft corporation%' OR lowerUTF8(org) ILIKE '%msft%', 'Azure',
      lowerUTF8(org) ILIKE '%oracle cloud%' OR lowerUTF8(org) ILIKE '%oracle america%' OR lowerUTF8(org) ILIKE '%oracle corporation%' OR lowerUTF8(org) ILIKE '%oci%', 'Oracle Cloud',
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

      -- Generic hosting/CDN hints, plus named hosters whose brand carries no such hint.
      -- These deliberately land in the existing 'Hosting/CDN (Other)' bucket rather than
      -- getting buckets of their own: int_esg_node_classification enumerates bucket names
      -- explicitly and falls through to node_category='unknown', so a new bucket name would
      -- make a correctly-identified datacenter classify WORSE than an unrecognised one.
      lowerUTF8(org) ILIKE '%datacenter%' OR lowerUTF8(org) ILIKE '%data center%' OR lowerUTF8(org) ILIKE '%colo%' OR lowerUTF8(org) ILIKE '%hosting%' OR lowerUTF8(org) ILIKE '%vps%' OR lowerUTF8(org) ILIKE '%server%' OR lowerUTF8(org) ILIKE '%incapsula%' OR lowerUTF8(org) ILIKE '%imperva%' OR
      lowerUTF8(org) ILIKE '%contabo%' OR lowerUTF8(org) ILIKE '%netcup%' OR lowerUTF8(org) ILIKE '%ionos%' OR lowerUTF8(org) ILIKE '%1&1 internet%' OR lowerUTF8(org) ILIKE '%hostpapa%' OR
      lowerUTF8(org) ILIKE '%constant company%' OR lowerUTF8(org) ILIKE '%mevspace%' OR lowerUTF8(org) ILIKE '%rackdog%' OR lowerUTF8(org) ILIKE '%evanzo%' OR lowerUTF8(org) ILIKE '%interserver%' OR
      lowerUTF8(org) ILIKE '%m247%' OR lowerUTF8(org) ILIKE '%datacamp%' OR lowerUTF8(org) ILIKE '%teraswitch%' OR lowerUTF8(org) ILIKE '%limestone networks%' OR lowerUTF8(org) ILIKE '%31173 services%' OR
      lowerUTF8(org) ILIKE '%latitude.sh%' OR lowerUTF8(org) ILIKE '%nubes, llc%' OR lowerUTF8(org) ILIKE '%allnodes%' OR lowerUTF8(org) ILIKE '%tzulo%' OR lowerUTF8(org) ILIKE '%gthost%' OR
      lowerUTF8(org) ILIKE '%global layer%' OR lowerUTF8(org) ILIKE '%baxet%' OR lowerUTF8(org) ILIKE '%dpkgsoft%' OR lowerUTF8(org) ILIKE '%signet b.v%' OR lowerUTF8(org) ILIKE '%leaseweb%' OR
      lowerUTF8(org) ILIKE '%worldstream%' OR lowerUTF8(org) ILIKE '%serverius%' OR lowerUTF8(org) ILIKE '%aeza%' OR lowerUTF8(org) ILIKE '%stark industries%' OR lowerUTF8(org) ILIKE '%melbikomas%' OR
      lowerUTF8(org) ILIKE '%huawei cloud%' OR lowerUTF8(org) ILIKE '%tencent%' OR lowerUTF8(org) ILIKE '%internap%' OR lowerUTF8(org) ILIKE '%timeweb%' OR lowerUTF8(org) ILIKE '%webnx%' OR
      lowerUTF8(org) ILIKE '%uk-2 limited%' OR lowerUTF8(org) ILIKE '%ip-projects%' OR lowerUTF8(org) ILIKE '%wholesail%' OR lowerUTF8(org) ILIKE '%oneprovider%' OR lowerUTF8(org) ILIKE '%zomro%', 'Hosting/CDN (Other)',

      -- Residential & office ISPs. Generic lexicon first, then named carriers whose brand
      -- contains none of it (incl. non-English: comunicacoes / comunicaciones / telefonia).
      lowerUTF8(org) ILIKE '%telecom%' OR lowerUTF8(org) ILIKE '%telekom%' OR lowerUTF8(org) ILIKE '%telecommunications%' OR lowerUTF8(org) ILIKE '%communications%' OR lowerUTF8(org) ILIKE '%wireless%' OR
      lowerUTF8(org) ILIKE '%cable%' OR lowerUTF8(org) ILIKE '%broadband%' OR lowerUTF8(org) ILIKE '%internet%' OR lowerUTF8(org) ILIKE '%fibre%' OR lowerUTF8(org) ILIKE '%fiber%' OR lowerUTF8(org) ILIKE '%mobile%' OR
      -- Stems, not full words: lowerUTF8() lowercases accents but ILIKE matches literal
      -- bytes, so '%telefon%' misses both 'TELEFONICA BRASIL' and 'Telefonos'. One stem is
      -- not enough because the accent sits in a different place per language --
      -- 'telefonica' keeps "telef" but 'telefonos' breaks it at "tel-e". Hence a stem on
      -- each side of the accent. Kept ASCII deliberately: some scripts/checks readers open
      -- model SQL without an explicit encoding and die on cp1252.
      lowerUTF8(org) ILIKE '%telef%' OR lowerUTF8(org) ILIKE '%fono%' OR lowerUTF8(org) ILIKE '%fonia%' OR
      lowerUTF8(org) ILIKE '%telephone%' OR lowerUTF8(org) ILIKE '%comunica%' OR lowerUTF8(org) ILIKE '%citynetz%' OR lowerUTF8(org) ILIKE '%netcom%' OR
      lowerUTF8(org) ILIKE '%space exploration technologies%' OR lowerUTF8(org) ILIKE '%starlink%' OR lowerUTF8(org) ILIKE '%versatel%' OR lowerUTF8(org) ILIKE '%vodafone%' OR lowerUTF8(org) ILIKE '%orange %' OR
      lowerUTF8(org) ILIKE '%at&t%' OR lowerUTF8(org) ILIKE '%verizon%' OR lowerUTF8(org) ILIKE '%bell canada%' OR lowerUTF8(org) ILIKE '%swisscom%' OR lowerUTF8(org) ILIKE '%proximus%' OR
      lowerUTF8(org) ILIKE '%virgin media%' OR lowerUTF8(org) ILIKE '%sky uk%' OR lowerUTF8(org) ILIKE '%kpn b.v%' OR lowerUTF8(org) ILIKE '%altibox%' OR lowerUTF8(org) ILIKE '%hyperoptic%' OR
      lowerUTF8(org) ILIKE '%digi romania%' OR lowerUTF8(org) ILIKE '%free sas%' OR lowerUTF8(org) ILIKE '%sunrise gmbh%' OR lowerUTF8(org) ILIKE '%metronet%' OR lowerUTF8(org) ILIKE '%post luxembourg%' OR
      lowerUTF8(org) ILIKE '%luxembourg online%' OR lowerUTF8(org) ILIKE '%odido%' OR lowerUTF8(org) ILIKE '%yettel%' OR lowerUTF8(org) ILIKE '%wilhelm.tel%' OR lowerUTF8(org) ILIKE '%htp gmbh%' OR
      lowerUTF8(org) ILIKE '%uninet%' OR lowerUTF8(org) ILIKE '%tot public company%' OR lowerUTF8(org) ILIKE '%true online%' OR lowerUTF8(org) ILIKE '%digiweb%' OR lowerUTF8(org) ILIKE '%nos comunicacoes%' OR
      lowerUTF8(org) ILIKE '%chunghwa%' OR lowerUTF8(org) ILIKE '%data communication business group%' OR lowerUTF8(org) ILIKE '%tm technology services%' OR lowerUTF8(org) ILIKE '%meo -%' OR
      lowerUTF8(org) ILIKE '%videotron%' OR lowerUTF8(org) ILIKE '%telenet%' OR lowerUTF8(org) ILIKE '%myrepublic%' OR lowerUTF8(org) ILIKE '%vocus%' OR lowerUTF8(org) ILIKE '%starhub%' OR
      lowerUTF8(org) ILIKE '%softbank%' OR lowerUTF8(org) ILIKE '%plusnet%' OR lowerUTF8(org) ILIKE '%green.ch%' OR lowerUTF8(org) ILIKE '%glasfaser%' OR lowerUTF8(org) ILIKE '%a1 bulgaria%' OR
      lowerUTF8(org) ILIKE '%init7%' OR lowerUTF8(org) ILIKE '%vimpelcom%' OR lowerUTF8(org) ILIKE '%viettel%' OR lowerUTF8(org) ILIKE '%hkt limited%' OR lowerUTF8(org) ILIKE '%edpnet%' OR
      lowerUTF8(org) ILIKE '%mts pjsc%' OR lowerUTF8(org) ILIKE '%telenor%' OR lowerUTF8(org) ILIKE '%superloop%' OR lowerUTF8(org) ILIKE '%ebox%' OR lowerUTF8(org) ILIKE '%cincinnati bell%' OR
      lowerUTF8(org) ILIKE '%breezeline%' OR lowerUTF8(org) ILIKE '%spark new zealand%' OR lowerUTF8(org) ILIKE '%ecotel%' OR lowerUTF8(org) ILIKE '%o2 business%' OR lowerUTF8(org) ILIKE '%virgin plus%', 'Public ISP (Home/Office)',

      -- No positive evidence either way. This MUST NOT default to a residential label:
      -- doing so converts absence of evidence into a positive claim, and the claim it
      -- manufactures ('home_staker' downstream, at 0.80 confidence) is the flattering one
      -- for a decentralisation metric. Before this branch existed, most of the
      -- 'Public ISP (Home/Office)' bucket had matched no residential pattern at all, the
      -- largest single block of it being a budget VPS provider. 'Unknown' is a real
      -- answer; int_esg_node_classification already scores it at 0.30 confidence. Prefer
      -- widening the lists above over narrowing this branch.
      'Unknown'
  ) AS generic_provider
  FROM `crawlers_data`.`ipinfo` 
)

SELECT * FROM source