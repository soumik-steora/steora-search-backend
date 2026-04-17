import time
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.contrib.admin.views.decorators import staff_member_required
from django.utils.decorators import method_decorator
from .services import search_transcripts, advanced_search_transcripts, get_court_locations
from .analytics import log_search


class TranscriptSearchView(APIView):
    """Original broad-text search (kept for backward compatibility)."""

    def get(self, request):
        start_time = time.time()

        q = request.GET.get('q', '').strip()
        page = int(request.GET.get('page', 1))
        limit = int(request.GET.get('limit', 20))
        offset = (page - 1) * limit

        filters = {k: v for k, v in {
            'courtLocation':      request.GET.get('courtLocation'),
            'verified':           request.GET.get('verified'),
            'startDate':          request.GET.get('startDate'),
            'endDate':            request.GET.get('endDate'),
            'banYcja':            request.GET.get('banYcja'),
            'banSealedRecording': request.GET.get('banSealedRecording'),
            'banPublication':     request.GET.get('banPublication'),
            'banNoPublication':   request.GET.get('banNoPublication'),
        }.items() if v is not None and v != ''}

        results, total_count = search_transcripts(q, limit, offset, filters)
        latency_ms = int((time.time() - start_time) * 1000)

        log_search(
            request,
            search_type="basic",
            query=q,
            court_location=filters.get("courtLocation", ""),
            results_count=total_count,
            latency_ms=latency_ms,
        )

        return Response({
            "query": q,
            "results": results,
            "count": len(results),
            "total_count": total_count,
            "meta": {"page": page, "limit": limit, "latency_ms": latency_ms},
        })


class CourtLocationsView(APIView):
    """Return the sorted list of all distinct court locations."""

    def get(self, request):
        locations = get_court_locations()
        return Response({"locations": locations})


class AdvancedSearchView(APIView):
    """
    Advanced search driven by three structured inputs:
      - courtLocation  (required)
      - styleOfCause   (optional, fuzzy)
      - dateRange      (optional: last30/60/90/180, single, multiple)
    """

    def get(self, request):
        start_time = time.time()

        court_location  = request.GET.get('courtLocation', '').strip()
        style_of_cause  = request.GET.get('styleOfCause', '').strip()
        date_range_type = request.GET.get('dateRangeType', '').strip()
        single_date     = request.GET.get('singleDate', '').strip()
        dates_raw       = request.GET.get('dates', '').strip()
        multiple_dates  = [d.strip() for d in dates_raw.split(',') if d.strip()] if dates_raw else []

        page   = int(request.GET.get('page', 1))
        limit  = int(request.GET.get('limit', 20))
        offset = (page - 1) * limit

        if not court_location:
            return Response(
                {"error": "courtLocation is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        results, total_count = advanced_search_transcripts(
            court_location  = court_location,
            style_of_cause  = style_of_cause,
            date_range_type = date_range_type,
            single_date     = single_date,
            multiple_dates  = multiple_dates,
            limit           = limit,
            offset          = offset,
        )

        latency_ms = int((time.time() - start_time) * 1000)

        log_search(
            request,
            search_type="advanced",
            query=style_of_cause,
            court_location=court_location,
            results_count=total_count,
            latency_ms=latency_ms,
        )

        return Response({
            "courtLocation":  court_location,
            "styleOfCause":   style_of_cause,
            "dateRangeType":  date_range_type,
            "results":        results,
            "count":          len(results),
            "total_count":    total_count,
            "meta": {
                "page":       page,
                "limit":      limit,
                "latency_ms": latency_ms,
            },
        })


@method_decorator(staff_member_required, name="dispatch")
class AnalyticsView(APIView):
    """
    Read-only analytics endpoint — staff only (is_staff=True required).

    GET /api/analytics/
    Query params:
      page        int  (default 1)
      limit       int  (default 50, max 200)
      search_type basic|advanced
      ip          filter by IP prefix
      country     filter by country name
      is_proxy    true|false
      date_from   YYYY-MM-DD
      date_to     YYYY-MM-DD
    """

    def get(self, request):
        from .models import SearchAnalytics
        from django.utils.dateparse import parse_date
        from django.utils import timezone
        import datetime

        qs = SearchAnalytics.objects.all()

        search_type = request.GET.get("search_type", "").strip()
        if search_type in ("basic", "advanced"):
            qs = qs.filter(search_type=search_type)

        ip_filter = request.GET.get("ip", "").strip()
        if ip_filter:
            qs = qs.filter(ip_address__startswith=ip_filter)

        country = request.GET.get("country", "").strip()
        if country:
            qs = qs.filter(country__icontains=country)

        if request.GET.get("is_proxy") == "true":
            qs = qs.filter(is_proxy=True)

        date_from = parse_date(request.GET.get("date_from", ""))
        date_to   = parse_date(request.GET.get("date_to", ""))
        if date_from:
            qs = qs.filter(timestamp__date__gte=date_from)
        if date_to:
            qs = qs.filter(timestamp__date__lte=date_to)

        try:
            page  = max(1, int(request.GET.get("page", 1)))
            limit = min(200, max(1, int(request.GET.get("limit", 50))))
        except (ValueError, TypeError):
            page, limit = 1, 50

        total = qs.count()
        offset = (page - 1) * limit
        records = qs[offset: offset + limit]

        data = [
            {
                "id":            r.id,
                "timestamp":     r.timestamp.isoformat(),
                "ip_address":    r.ip_address,
                "city":          r.city,
                "region":        r.region,
                "country":       r.country,
                "country_code":  r.country_code,
                "latitude":      float(r.latitude) if r.latitude is not None else None,
                "longitude":     float(r.longitude) if r.longitude is not None else None,
                "isp":           r.isp,
                "is_proxy":      r.is_proxy,
                "is_hosting":    r.is_hosting,
                "browser":       r.browser,
                "os":            r.os,
                "device_type":   r.device_type,
                "search_type":   r.search_type,
                "query":         r.query,
                "court_location": r.court_location,
                "results_count": r.results_count,
                "latency_ms":    r.latency_ms,
            }
            for r in records
        ]

        return Response({
            "total": total,
            "page":  page,
            "limit": limit,
            "results": data,
        })