# coding=utf8

from django.db import models
from django.contrib.auth.models import AbstractBaseUser,BaseUserManager
from django.contrib.auth.hashers import make_password,check_password
from django.utils import timezone

import uuid

# 自定义用户管理
class CustomUserManager(BaseUserManager):

    