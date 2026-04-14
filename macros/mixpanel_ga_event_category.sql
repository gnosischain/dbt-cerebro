{#
  mixpanel_ga_event_category(event_name, is_autocapture)
  -------------------------------------------------------
  Returns a ClickHouse multiIf(...) expression that classifies a Gnosis App
  Mixpanel event into one of the following categories:

    pageview    — $mp_web_page_view automatic page-view events
    modal       — Open Modal / Open Bottom Sheet events
    login       — Login with Passkey
    feature     — Explicit product-feature events (Circles mint, Marketplace
                  Purchase, QR Scanner, etc.)
    navigation  — Button-level navigation (back / Back / Close ariaLabel clicks)
    system      — Any other Mixpanel system event starting with '$'
    action      — Auto-captured button/interaction events (is_autocapture = 1)
    other       — Anything that doesn't match the above

  Usage
  -----
    {{ mixpanel_ga_event_category() }}                      AS event_category
    {{ mixpanel_ga_event_category('e.event_name', 'e.is_autocapture') }} AS event_category

  To add or rename categories: edit ONLY this macro file.
#}
{% macro mixpanel_ga_event_category(event_name='event_name', is_autocapture='is_autocapture') %}
multiIf(
    {{ event_name }} = '$mp_web_page_view',                                            'pageview',
    {{ event_name }} IN ('Open Modal', 'Open Bottom Sheet'),                           'modal',
    {{ event_name }} = 'Login with Passkey',                                           'login',
    {{ event_name }} IN (
        'Success - Circles mint',
        'Marketplace Purchase',
        'Request QR Scanner',
        'Close QR Scanner',
        'QR Scan',
        'QR Scan Passkey Validation URL',
        'QR Scan Transaction URL'
    ),                                                                                 'feature',
    {{ event_name }} IN ('back', 'Back', 'Close'),                                     'navigation',
    startsWith({{ event_name }}, '$'),                                                 'system',
    {{ is_autocapture }} = 1,                                                          'action',
    'other'
)
{% endmacro %}
