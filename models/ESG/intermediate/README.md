# Enhanced Node Population Estimation for Gnosis Network

## Overview

This document describes the implementation of an advanced node population estimation system that combines traditional Chao-1 statistical methods with failure analysis to provide comprehensive estimates of the Gnosis network size.

## Background

Traditional peer-to-peer network analysis only counts successfully contacted nodes, missing a significant portion of the network that exists but is unreachable due to NAT, firewalls, or temporary connectivity issues. Our enhanced methodology addresses this limitation by:

1. **Statistical estimation** of hidden successful nodes using Chao-1 estimators
2. **Failure pattern analysis** to estimate additional reachable nodes from failed connections
3. **Multi-layer population estimates** providing different perspectives on network size

## Mathematical Foundation

### Chao-1 Estimator

The Chao-1 estimator, originally developed for species richness estimation in ecology, estimates the total population size based on observation frequencies:

```
S_chao1 = S_obs + (f1² / (2 × f2))

Where:
- S_obs = Number of unique nodes observed
- f1 = Number of nodes seen exactly once (singletons)
- f2 = Number of nodes seen exactly twice (doubletons)
```

**Bias-corrected version** (used when f2 = 0):
```
S_chao1 = S_obs + (f1 × (f1 - 1)) / 2
```

**Variance estimation** for confidence intervals:
```
Var(S_chao1) = f2 × ((f1/f2)² / 2 + (f1/f2)³ + (f1/f2)⁴ / 4)
```

### Failure Analysis Enhancement

We extend the traditional approach by analyzing connection failures and estimating reachability probabilities:

```
Additional_Reachable = Σ(Failure_Type_Count × Reachability_Probability)

Where Reachability_Probability varies by failure type:
- Timeout: 0.30 (often temporary network issues)
- Connection Refused: 0.10 (firewall/NAT blocking)
- Unreachable: 0.05 (network routing issues)
- Protocol Mismatch: 0.80 (wrong protocol version)
- Dial Error: 0.20 (various connection issues)
```

## Implementation Architecture

### Data Flow

```
Raw P2P Discovery Data
         ↓
Connection Status Analysis
    ├── Successful Connections
    └── Failed Connections (by type)
         ↓
Statistical Analysis
    ├── Chao-1 (Successful Only)
    ├── Chao-1 (All Attempts)
    └── Failure Reachability Analysis
         ↓
Multi-Layer Estimates
    ├── Conservative (Successful Only)
    ├── Enhanced (Successful + Recoverable)
    └── Maximum (All Discovered)
```

### Core CTEs (Common Table Expressions)

#### 1. `peer_connection_analysis`
- Categorizes each connection attempt as successful/failed
- Classifies failure types (timeout, refused, unreachable, etc.)
- Source: `int_p2p_discv5_peers` table

#### 2. `successful_chao1` & `successful_stats`
- Applies Chao-1 estimator to successful connections only
- Calculates f1, f2, and population estimate
- Most conservative estimate

#### 3. `all_attempts_chao1` & `all_attempts_stats`
- Applies Chao-1 to all discovered peers (successful + failed)
- Provides maximum network size estimate
- Includes truly unreachable nodes

#### 4. `peer_status_summary` & `failed_only_peers`
- Identifies peers that only had failed connections
- Avoids correlated subqueries for ClickHouse compatibility

#### 5. `failure_analysis`
- Counts peers by failure type
- Applies reachability probabilities
- Estimates additional recoverable nodes

#### 6. `failure_reachability`
- Aggregates failure analysis across all types
- Calculates weighted sum of recoverable nodes
- Separate CTE to avoid GROUP BY conflicts

## Output Metrics

### Core Population Estimates

| Metric | Description | Use Case |
|--------|-------------|----------|
| `observed_successful_nodes` | Nodes we can successfully connect to | Baseline count (matches daily peer analysis) |
| `chao1_successful_only` | Statistical estimate of total reachable nodes | Conservative estimate for capacity planning |
| `enhanced_total_reachable` | Successful + recoverable from failures | Realistic estimate for network health |
| `estimated_network_size` | All discovered peers (including unreachable) | Maximum network participation estimate |

### Hidden Node Analysis

| Metric | Description |
|--------|-------------|
| `hidden_successful_nodes` | Additional reachable nodes we're missing |
| `hidden_successful_pct` | Percentage of reachable nodes that are hidden |
| `estimated_additional_reachable` | Nodes recoverable from failed connections |

### Network Health Metrics

| Metric | Description |
|--------|-------------|
| `connection_success_rate_pct` | Percentage of discovery attempts that succeed |
| `network_discovery_coverage_pct` | How much of total network we can discover |
| `reachable_discovery_coverage_pct` | How much of reachable network we contact |

### Diagnostic Information

| Metric | Description |
|--------|-------------|
| `successful_singletons` (f1) | Nodes seen exactly once successfully |
| `successful_doubletons` (f2) | Nodes seen exactly twice successfully |
| `all_singletons` | All peers (successful + failed) seen once |
| `all_doubletons` | All peers seen twice |
| `failed_only_peers` | Peers that never had successful connections |

## Expected Results

Based on Gnosis network characteristics:

```
Daily Estimates (typical values):
├── Observed Successful: ~1,200 nodes
├── Chao-1 Successful: ~1,500 nodes (+25% hidden successful)
├── Enhanced Reachable: ~1,800 nodes (+50% including recoverable)
└── Total Network Size: ~2,200 nodes (+83% all discovered)

Success Rates:
├── Connection Success: ~75-85%
├── Discovery Coverage: ~85-95% of total network
└── Reachable Coverage: ~65-75% of reachable nodes
```

## Statistical Confidence

### Confidence Intervals
All estimates include 95% confidence intervals calculated using:
- Log-normal distribution (appropriate for species richness)
- Lower bound constrained to not fall below observed count
- Standard errors from Chao-1 variance formula

### Sample Quality Indicators
- **Complete Sample**: f1 = 0 (no singletons, perfect discovery)
- **Good Coverage**: >95% sample coverage
- **High Uncertainty**: f2 = 0 (no doubletons, unreliable estimates)

## Data Quality Requirements

### Input Data Filtering
```sql
WHERE
    toStartOfDay(visit_ended_at) < today()  -- Exclude partial current day
    AND empty(dial_errors) = 1              -- Successful connections only (for baseline)
    AND crawl_error IS NULL                 -- No crawl errors (for baseline)
```

### Temporal Scope
- **Daily aggregation**: One estimate per day
- **Historical analysis**: Monthly incremental processing
- **Lookback period**: Configurable via `apply_monthly_incremental_filter`

## Implementation Notes

### ClickHouse Compatibility
- Explicit type casting (`toFloat64`, `toUInt64`) for mixed arithmetic
- `COALESCE` for NULL handling in OUTER JOINs
- Separate CTEs to avoid aggregation/GROUP BY conflicts
- Array functions: `arraySlice()`, `arrayElement()`

### Performance Optimizations
- Incremental processing with date partitioning
- Indexed lookups on observation_date
- Materialized intermediate tables for complex calculations

### Error Handling
- Graceful degradation when f2 = 0 (no doubletons)
- Bounds checking (estimates cannot be less than observed)
- Type safety with explicit casting

## Usage Examples

### Basic Query
```sql
SELECT 
    observation_date,
    observed_successful_nodes,
    chao1_successful_only,
    enhanced_total_reachable,
    hidden_successful_pct
FROM {{ ref('int_esg_node_population_chao1') }}
WHERE observation_date >= today() - 30
ORDER BY observation_date DESC;
```

### Network Health Assessment
```sql
SELECT 
    observation_date,
    connection_success_rate_pct,
    network_discovery_coverage_pct,
    CASE 
        WHEN connection_success_rate_pct > 80 THEN 'Healthy'
        WHEN connection_success_rate_pct > 60 THEN 'Moderate'
        ELSE 'Concerning'
    END AS network_health_status
FROM {{ ref('int_esg_node_population_chao1') }}
ORDER BY observation_date DESC
LIMIT 7;
```

### Trend Analysis
```sql
SELECT 
    toStartOfWeek(observation_date) AS week,
    AVG(enhanced_total_reachable) AS avg_reachable_nodes,
    AVG(hidden_successful_pct) AS avg_hidden_pct
FROM {{ ref('int_esg_node_population_chao1') }}
WHERE observation_date >= today() - 90
GROUP BY week
ORDER BY week DESC;
```

## Integration with Carbon Footprint Models

This population estimate feeds into downstream carbon footprint calculations:

1. **Node Count Multiplier**: `enhanced_total_reachable / observed_successful_nodes`
2. **Uncertainty Propagation**: Confidence intervals flow through Monte Carlo simulations
3. **Geographic Distribution**: Applied to all population estimates
4. **Power Consumption**: Scaled by estimated total reachable nodes

## References

1. **Chao, A. (1984).** "Nonparametric estimation of the number of classes in a population." *Scandinavian Journal of Statistics*, 11(4), 265-270.

2. **Chao, A., & Chiu, C. H. (2016).** "Species richness: estimation and comparison." *Wiley StatsRef: Statistics Reference Online*, 1-26.

3. **Colwell, R. K., & Coddington, J. A. (1994).** "Estimating terrestrial biodiversity through extrapolation." *Philosophical Transactions of the Royal Society B*, 345(1311), 101-118.

4. **Gotelli, N. J., & Colwell, R. K. (2001).** "Quantifying biodiversity: procedures and pitfalls in the measurement and comparison of species richness." *Ecology Letters*, 4(4), 379-391.

## Future Enhancements

### Short-term
- Real-time confidence interval updates
- Failure type classification improvements
- Integration with validator registry data

### Medium-term
- Machine learning for reachability probability refinement
- Cross-validation with independent discovery methods
- Temporal pattern analysis for better estimates

### Long-term
- Multi-chain comparative analysis
- Predictive modeling for network growth
- Integration with economic incentive models

---

**Model Location**: `models/ESG/intermediate/int_esg_node_population_chao1.sql`  
**Dependencies**: `int_p2p_discv5_peers`, `apply_monthly_incremental_filter` macro  
**Output**: Daily population estimates with confidence intervals and diagnostic metrics



# Carbon Intensity Ensemble Model

## Overview

The `int_esg_carbon_intensity_ensemble` model transforms monthly carbon intensity data from Ember into enhanced estimates with sophisticated uncertainty quantification for Monte Carlo ESG carbon footprint calculations. This model addresses the temporal and geographic variability inherent in electricity grid emissions that simple monthly averages cannot capture.

## Problem Statement

**Challenge**: Monthly carbon intensity averages hide significant temporal variation that affects carbon footprint accuracy:

- **Daily cycles**: Grid emissions vary throughout the day (renewables vs. fossil backup)
- **Seasonal patterns**: Heating/cooling demand drives different generation mixes
- **Geographic variation**: Climate zones and hemispheres have opposite seasonal patterns
- **Grid composition**: High-renewable grids are more variable than fossil-heavy grids

**Solution**: Generate uncertainty-enhanced monthly estimates that capture this hidden variation for realistic Monte Carlo simulations.

## Data Sources

### Primary Input
- **Ember Global Electricity Data**: Monthly carbon intensity by country (gCO2/kWh)
- **Coverage**: 200+ countries, 2000-present
- **Quality**: Authoritative data from national statistics and energy agencies

### Key Fields Used
```sql
"Date"           -- Monthly timestamp
"ISO 3 code"     -- 3-letter country code (DEU, USA, etc.)
"Continent"      -- Geographic continent classification
"Value"          -- Carbon intensity in gCO2/kWh
"Unit"           -- Data unit filter (gCO2/kWh only)
```

## Model Architecture

### Data Flow
```
Raw Ember Data (Monthly)
        ↓
Country/Continent Filtering
        ↓
Grid-Type Classification
        ↓
Temporal Uncertainty Modeling
        ↓
Seasonal Adjustment (by Continent)
        ↓
Confidence Intervals & Quality Metrics
        ↓
Enhanced Monthly Estimates with Uncertainty
```

## Core Logic

### 1. Grid-Type Based Uncertainty

Carbon intensity uncertainty varies by grid composition:

```sql
-- Higher renewable penetration = higher temporal variability
CASE 
    WHEN carbon_intensity < 100  THEN 0.25  -- Low carbon (wind/solar heavy)
    WHEN carbon_intensity < 300  THEN 0.20  -- Medium carbon (mixed)
    WHEN carbon_intensity < 600  THEN 0.15  -- High carbon (gas dominant)
    ELSE 0.12                               -- Very high carbon (coal baseload)
END * carbon_intensity AS temporal_uncertainty
```

**Rationale**: 
- **Low-carbon grids** (Norway, France): High renewable variability (±25%)
- **High-carbon grids** (Poland, India): Stable fossil baseload (±12%)

### 2. Measurement Uncertainty

Additional 10% uncertainty accounts for:
- Monthly averaging effects
- Data collection methodology
- Regional aggregation within countries

### 3. Continental Seasonal Adjustments

**Europe** (Heating-Dominant Northern):
```
Winter (Dec-Feb): +18% -- Peak heating demand
Summer (Jun-Aug): -8%  -- Low demand + solar peak
Shoulder seasons: +8%   -- Moderate adjustment
```

**Asia** (Mixed Climate Zones):
```
Winter: +12% -- Industrial + heating
Summer: +8%  -- Cooling demand
Spring/Fall: +5% -- Moderate seasons
```

**North America** (Balanced Heating/Cooling):
```
Winter: +15% -- Heating peak
Summer: +12% -- Cooling peak
Shoulder: +3% -- Balanced demand
```

**Oceania** (Southern Hemisphere):
```
Jun-Aug: +15%  -- Southern winter
Dec-Feb: -5%   -- Southern summer (reversed)
Shoulder: +5%  -- Moderate seasons
```

**South America** (Southern + Tropical):
```
Jun-Aug: +10% -- Milder southern winter
Dec-Feb: -2%  -- Tropical + southern summer
```

**Africa** (Tropical + Mixed):
```
Jun-Aug: +5% -- Dry season effects
Dec-Feb: -2% -- Wet season
Minimal overall variation
```

## Output Schema

### Core Metrics
| Column | Type | Description | Use Case |
|--------|------|-------------|----------|
| `carbon_intensity_mean` | Float | Seasonally adjusted monthly mean | Point estimate for calculations |
| `carbon_intensity_std` | Float | Combined uncertainty standard deviation | Monte Carlo sampling parameter |
| `ci_lower_95` / `ci_upper_95` | Float | 95% confidence intervals | Uncertainty bounds |
| `ci_lower_90` / `ci_upper_90` | Float | 90% confidence intervals | Conservative bounds |

### Uncertainty Analysis
| Column | Description |
|--------|-------------|
| `coefficient_of_variation` | Relative uncertainty (std/mean) |
| `uncertainty_category` | Low/Medium/High classification |
| `temporal_std` | Grid variability component |
| `measurement_std` | Data quality component |

### Geographic & Quality
| Column | Description |
|--------|-------------|
| `continent` | Source continent for seasonal adjustment |
| `seasonal_adjustment` | Applied seasonal factor |
| `confidence_score` | Data quality indicator (0-1) |
| `sources_used` | Data provenance array |

## Mathematical Framework

### Combined Uncertainty
```
Total_Std = √(Temporal_Uncertainty² + Measurement_Uncertainty²)

Where:
Temporal_Uncertainty = f(grid_composition) × base_carbon_intensity
Measurement_Uncertainty = 0.10 × base_carbon_intensity
```

### Seasonal Adjustment
```
Adjusted_CI = Base_CI × Seasonal_Factor(continent, month)

Where Seasonal_Factor varies by:
- Continent (climate patterns)
- Month (seasonal timing)
- Hemisphere (Northern vs Southern)
```

### Confidence Intervals
```
CI_95 = Mean ± 1.96 × Total_Std
CI_90 = Mean ± 1.645 × Total_Std
```

## Integration with ESG Models

### Monte Carlo Sampling
```sql
-- In downstream carbon footprint models:
sampled_ci = carbon_intensity_mean + 
            normal_random() × carbon_intensity_std × temporal_factor
```

### Expected Uncertainty Levels
- **Low-carbon grids**: 15-30% relative uncertainty
- **Medium-carbon grids**: 12-25% relative uncertainty  
- **High-carbon grids**: 8-18% relative uncertainty

## Performance Optimizations

### Partitioning Strategy
```sql
partition_by='toStartOfYear(month_date)'  -- Yearly partitions
unique_key='(month_date, country_code)'   -- Monthly granularity
```

**Rationale**: Avoids ClickHouse partition limits while maintaining query performance.

### Incremental Processing
```sql
{{ apply_monthly_incremental_filter('"Date"','month_date','true') }}
```

Only processes new/updated months, reducing computation time.

## Data Quality Checks

### Input Validation
- Non-null country codes and carbon intensity values
- Positive carbon intensity values only
- Valid unit filtering (gCO2/kWh)

### Output Validation
- Confidence intervals properly ordered (lower < mean < upper)
- Reasonable uncertainty bounds (5-50% coefficient of variation)
- Seasonal factors within expected ranges (0.85-1.25)

## Usage Examples

### Basic Query
```sql
SELECT 
    month_date,
    country_code,
    carbon_intensity_mean,
    carbon_intensity_std,
    uncertainty_category
FROM {{ ref('int_esg_carbon_intensity_ensemble') }}
WHERE month_date >= '2024-01-01'
ORDER BY carbon_intensity_mean DESC;
```

### Monte Carlo Sampling
```sql
WITH carbon_samples AS (
    SELECT
        country_code,
        carbon_intensity_mean + 
        (rand() - 0.5) * 2 * 1.96 * carbon_intensity_std AS sampled_ci
    FROM {{ ref('int_esg_carbon_intensity_ensemble') }}
    WHERE month_date = '2024-12-01'
)
SELECT 
    country_code,
    AVG(sampled_ci) as mean_sampled_ci,
    STDDEV(sampled_ci) as std_sampled_ci
FROM carbon_samples
GROUP BY country_code;
```

### Uncertainty Analysis
```sql
SELECT 
    continent,
    AVG(coefficient_of_variation) as avg_uncertainty,
    COUNT(*) as country_count
FROM {{ ref('int_esg_carbon_intensity_ensemble') }}
WHERE month_date = '2024-12-01'
GROUP BY continent
ORDER BY avg_uncertainty DESC;
```

## Future Enhancements

### Short-term
- **Multiple data sources**: Integrate ElectricityMaps, WattTime for cross-validation
- **Sub-monthly updates**: Weekly or daily estimates where data available
- **Regional disaggregation**: State/province level for large countries

### Medium-term
- **Machine learning**: Predict seasonal patterns from historical data
- **Real-time integration**: Live grid data for current-day estimates
- **Marginal emissions**: Time-specific marginal vs average factors

### Long-term
- **Forward-looking estimates**: Grid decarbonization scenarios
- **Renewable forecasting**: Weather-dependent generation modeling
- **Economic dispatch modeling**: Merit order effects on emissions

## Dependencies

### dbt Models
- `stg_crawlers_data__ember_electricity_data`: Cleaned Ember data
- `apply_monthly_incremental_filter`: Incremental processing macro

### External Data
- Ember Global Electricity Review (annual updates)
- Future: ElectricityMaps API, WattTime API

## References

1. **Ember Climate**: Global Electricity Review methodology
2. **IPCC Guidelines**: Grid emission factors and uncertainty
3. **IEA Statistics**: Electricity generation and consumption patterns
4. **Grid Carbon Intensity Research**: Temporal variability studies

---

**Model Location**: `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql`  
**Materialization**: Incremental (monthly updates)  
**Dependencies**: `stg_crawlers_data__ember_electricity_data`  
**Output**: Monthly carbon intensity estimates with comprehensive uncertainty quantification