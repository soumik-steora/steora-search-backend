from django.contrib import admin
from django.utils.html import format_html

from .models import SearchAnalytics


@admin.register(SearchAnalytics)
class SearchAnalyticsAdmin(admin.ModelAdmin):
    list_display = (
        "timestamp",
        "ip_address",
        "flag_and_country",
        "city",
        "search_type_badge",
        "query_preview",
        "court_location",
        "results_count",
        "latency_ms",
        "device_type",
        "browser",
        "risk_flags",
    )
    list_filter = (
        "search_type",
        "device_type",
        "country",
        "is_proxy",
        "is_hosting",
        ("timestamp", admin.DateFieldListFilter),
    )
    search_fields = ("ip_address", "city", "country", "isp", "query", "court_location", "user_agent")
    readonly_fields = [f.name for f in SearchAnalytics._meta.get_fields()]
    ordering = ("-timestamp",)
    date_hierarchy = "timestamp"

    # Disable add / change / delete — analytics records are immutable security logs.
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser

    # --- custom display columns ---

    @admin.display(description="Country")
    def flag_and_country(self, obj):
        if not obj.country_code:
            return obj.country or "—"
        # Unicode flag emoji from country code
        code = obj.country_code.upper()
        flag = "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in code if c.isalpha())
        return format_html("{} {}", flag, obj.country)

    @admin.display(description="Type")
    def search_type_badge(self, obj):
        colour = "#0074D9" if obj.search_type == "advanced" else "#2ECC40"
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 8px;'
            'border-radius:3px;font-size:11px">{}</span>',
            colour,
            obj.get_search_type_display(),
        )

    @admin.display(description="Query")
    def query_preview(self, obj):
        q = obj.query or ""
        return (q[:60] + "…") if len(q) > 60 else q or "—"

    @admin.display(description="Risk")
    def risk_flags(self, obj):
        flags = []
        if obj.is_proxy:
            flags.append('<span style="color:#FF4136">⚠ Proxy</span>')
        if obj.is_hosting:
            flags.append('<span style="color:#FF851B">⚠ Hosting</span>')
        return format_html(" ".join(flags)) if flags else "—"
