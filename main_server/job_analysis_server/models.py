# coding=utf8
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager
from django.contrib.auth.hashers import make_password,check_password
from django.utils import timezone

import uuid

# Create your models here.

# 自定义用户管理
class CustomUserManager(BaseUserManager):

    # 创建普通用户
    def createUser(self,user_id,password=None):

        if not password:
            
            raise ValueError('password is required!')
        
        user = self.model(
            user_id=user_id,
            user_uuid = uuid.uuid4(),
            user_create = timezone.now(),
            user_update = timezone.now()
        )
        user.set_password(password)
        user.save()
        return user
    
    # 创建管理员用户
    def createAU(self,user_id,password=None):

        user = self.createUser(user_id,password)
        user.is_admin = True
        user.is_su = False
        user.save()
        return user
    
    # 创建超级用户
    def createSU(self,user_id,password=None):

        user = self.createUser(user_id,password)
        user.is_admin = True
        user.is_su = True
        user.save()
        return user
    
# 用户验证表
class UserAuth(AbstractBaseUser):

    user_id = models.CharField(max_length=8,unique=True,help_text='用户id')
    user_uuid = models.UUIDField(default=uuid.uuid4,editable=False)
    user_phone = models.CharField(max_length=16,default=0,help_text='手机号码')
    user_email = models.EmailField(help_text='用户邮箱')
    user_state = models.IntegerField(default=0,help_text='用户状态')
    is_su = models.BooleanField(default=False,help_text='是否超级管理员')
    is_admin = models.BooleanField(default=False,help_text='是否普通管理员')
    user_create = models.DateTimeField(auto_now_add=True,auto_now=False,help_text='创建时间')
    user_update = models.DateTimeField(auto_now_add=False,auto_now=False,help_text='修改时间')