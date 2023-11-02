# coding=utf8

from django.db import models
from django.contrib.auth.models import AbstractBaseUser,BaseUserManager
from django.contrib.auth.hashers import make_password,check_password
from django.utils import timezone

import uuid

# 自定义用户管理
class CustomUserManager(BaseUserManager):

    def createUser(self,user_phone=None,user_email=None,
                   vf_code=None,password=None,**extra_fields):
        
        if not (user_phone or user_email):
            raise ValueError('either phone or email is required!')
        
        # 待补充验证码逻辑
        
        if user_phone: extra_fields['user_phone'] = user_phone
        if user_email: extra_fields['user_email'] = user_email

        extra_fields.setdefault('user_id',str(uuid.uuid4())[:8])

        user = self.model(**extra_fields)
        user.user_create = timezone.now()
        user.user_update = timezone.now()
        
        if password: user.set_password(password)
        user.save()
        
        return user
    
    def createAU(self,user_phone=None,user_email=None,
                 vf_code=None,password=None,**extra_fields):
        
        user = self.createUser(user_phone,user_email,password,**extra_fields)
        user.user_authority = 1
        user.save()
        return user
    
    def createSU(self,user_phone=None,user_email=None,
                 vf_code=None,password=None,**extra_fields):
        
        user = self.createUser(user_phone,user_email,password,**extra_fields)
        user.user_authority = 2
        user.save()
        return user
    
class UserAuth(AbstractBaseUser):

    user_id = models.CharField(max_length=12,unique=True,editable=False,help_text='用户id')
    user_phone = models.CharField(max_length=16,default=0,help_text='手机号码')
    user_email= models.EmailField(help_text='用户邮箱')
    user_password = models.CharField(max_length=128,help_text='加密用户密码')
    user_create = models.DateTimeField(auto_now_add=False,auto_now=False,help_text='创建时间')
    user_update = models.DateTimeField(auto_now_add=False,auto_now=False,help_text='更新时间')
    user_authority = models.IntegerField(default=0,help_text='用户权限')
    user_state = models.IntegerField(default=0,help_text='用户状态')
    user_character = models.IntegerField(default=0,help_text='用户角色')

    objects = CustomUserManager()

    # 覆盖默认属性
    password = None
    last_login = None

    USERNAME_FIELD = 'user_id'
    REQUIRED_FIELDS = []

    def set_password(self,password):
        self.user_password = make_password(password)

    def check_password(self,password):
        return check_password(password,self.user_password)
    
    def check_vfcode(self,code):
        # 待补充逻辑
        return None
    
    class Meta:

        app_label = 'job_analysis_server'
        db_table = 'user_auth'