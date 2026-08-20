{% macro parse_gip_number(title_expr) -%}
{#
  Extract a GIP number only when the title *is* that GIP (leading identity),
  not when it merely mentions one mid-sentence.

  Accepts optional leading whitespace / zero-width chars / '#', bracket or
  paren prefixes ([Redo], (RE-RUN)), and "Re-do of:" / "Redo of:" before GIP.
  Separator after GIP may be "-", " ", "- ", or " - " (\\s*-?\\s*).
  Case-insensitive. Returns Nullable(UInt32).
#}
toUInt32OrNull(extract(
    {{ title_expr }},
    '(?i)^[\\s\\x{200B}\\x{FEFF}#]*(?:\\[[^\\]]*\\]\\s*)*(?:\\([^)]*\\)\\s*)*(?:re-?do of:\\s*)?GIP\\s*-?\\s*0*([0-9]+)'
))
{%- endmacro %}
