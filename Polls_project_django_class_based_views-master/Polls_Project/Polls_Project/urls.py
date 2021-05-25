"""Polls_Project URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from polls_app import views as poll_views
from rest_framework.views import APIView
from rest_framework.urlpatterns import format_suffix_patterns


urlpatterns = [
    path('admin/', admin.site.urls),
    path('', poll_views.HomeView.as_view(), name='home'),
    path('create/', poll_views.Create.as_view(), name='create'),
    #path('vote/<int:poll_id>/', poll_views.Vote.as_view(), name='vote'),
    path('results/<int:poll_id>/', poll_views.Result.as_view(), name='results'),
    path('vote/<int:poll_id>/<int:option_no>/', poll_views.voted, name='voted')
]
