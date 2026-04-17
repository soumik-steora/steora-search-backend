from django.urls import path
from .views import TranscriptSearchView, AdvancedSearchView, CourtLocationsView, AnalyticsView

urlpatterns = [
    path('search/',           TranscriptSearchView.as_view(), name='transcript-search'),
    path('search/advanced/',  AdvancedSearchView.as_view(),   name='advanced-search'),
    path('search/locations/', CourtLocationsView.as_view(),   name='court-locations'),
    path('analytics/',        AnalyticsView.as_view(),        name='search-analytics'),
]