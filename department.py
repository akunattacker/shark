import json
import logging
import math
from datetime import datetime
from functools import partial

from django.conf import settings
from django.core.paginator import Paginator, EmptyPage
from django.db import transaction
from django.db.models import OuterRef, Subquery, Count, IntegerField, Case, When, F, Q
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status as rest_status
from rest_framework.parsers import JSONParser
from rest_framework.views import APIView
from validate_email import validate_email

from src.account.models import Account
from src.accountcontact.models import AccountContact
from src.accountcontact.serializers import AccContactSerializer
from src.budgethistory.views import save_budget
from src.contact.models import Contact
from src.contact.models import ContactDepartmentApproval, ContactDepartmentRequestor
from src.contact.serializers import ContactSerializers, ContactDeptApprovalSerializer, ContactDeptRequestorSerializer
# load model
from src.department.models import Department
# load serializer
from src.department.serializers import departmentSerializers, getBudgetRemaining, UnitBussinessSerializers, \
    SubunitBussinessSerializers, ValidateGetUnitSerializers, get_last_budget_history
from src.helper.helpers import setDefaultValue, loginRequired, get_token_data, replaceAccountId, \
    check_access_account, nonstrict, get_label, check_account_for_IDOR
from src.helper.mail import send
# load helper
from src.helper.messages import UNIT_BUSSINESS, DATA_NOT_FOUND, SUCCESS_INVITE_DEPT
from src.helper.sharkresponse import response
# load view
from src.invitation import views as invitationView
from src.invitation.models import Invitation
from src.invitation.serializers import invitationSerializers
from src.logactivity.function import insert_log_activity
from src.opportunity.models import Opportunity
from src.quotation.models import Quotation
from src.transactionapproval.models import TransactionApproval
from src.user.views import get_user_id

log = logging.getLogger(__name__)


@method_decorator(csrf_exempt, name='dispatch')
class UnitBussinessView(APIView):
    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def post(self, request):
        data = request.data
        if not self.is_allowed(data['accountId']):
            return response(rest_status.HTTP_401_UNAUTHORIZED, message='Unauthorize')

        ub = UnitBussinessSerializers(data=data)
        if not ub.is_valid():
            return response(rest_status.HTTP_400_BAD_REQUEST, message="", errors=ub.errors)

        ub.save()

        return response(rest_status.HTTP_201_CREATED, ub.data, message="Success", status=True)

    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def put(self, request, id):
        data = request.data
        if not self.is_allowed(data['accountId']):
            return response(rest_status.HTTP_401_UNAUTHORIZED, message='Unauthorize')

        ub = UnitBussinessSerializers(get_object_or_404(Department, id=id), data=data)
        if not ub.is_valid():
            return response(rest_status.HTTP_400_BAD_REQUEST, message='', errors=ub.errors)

        ub.save()

        return response(rest_status.HTTP_201_CREATED, ub.data, message="Success", status=True)

    @partial(loginRequired, module="ACCOUNT", access="VIEW")
    def get(self, request, dept_id='', id=''):
        if id:
            return self.get_by_id(id)

        return self.get_all(dept_id)

    def get_by_id(self, id, message="Unit bussiness detail"):
        nonstrict()
        department = Department.objects.get(id=id)
        unit = UnitBussinessSerializers(department).data
        return response(rest_status.HTTP_200_OK, data=unit, message=message, status=True)

    def get_all(self, dept_id):
        serializer = ValidateGetUnitSerializers(data=self.request.GET)
        if not serializer.is_valid():
            return response(rest_status.HTTP_400_BAD_REQUEST, message='Get unit bussiness list failed',
                            errors=serializer.errors)

        page = serializer.data['page']
        limit = serializer.data['limit']
        search = serializer.data['search']
        account_id = serializer.data['accountId']

        if account_id:
            return self.get_all_by_account(account_id, page, limit)

        if not dept_id:
            return response(rest_status.HTTP_200_OK, message=UNIT_BUSSINESS,  status=True)

        query_filter = Q(parent_id=dept_id)
        if search:
            query_filter &= Q(code__icontains=search) | Q(name__icontains=search)

        nonstrict()
        units = Department.objects.filter(query_filter)
        paginator = Paginator(UnitBussinessSerializers(units, many=True).data, limit)
        meta = {
            "totalRecords": paginator.count,
            "totalPages": paginator.num_pages,
            "page": page,
            "limit": limit
        }

        return response(rest_status.HTTP_200_OK,
                        data=paginator.page(page).object_list, message=UNIT_BUSSINESS, meta=meta, status=True)

    def get_all_by_account(self, account_id, page, limit):
        try:
            token = get_token_data(self.request.META, key=None, all=True)
            if 'accountId' in token and token['accountId'] != account_id:
                return response(rest_status.HTTP_200_OK, data=[], message=DATA_NOT_FOUND)
            units = Department.objects.filter(type='UNIT', account_id=account_id)
            paginator = Paginator(UnitBussinessSerializers(units, many=True).data, limit)
            meta = {
                "totalRecords": paginator.count,
                "totalPages": paginator.num_pages,
                "page": page,
                "limit": limit
            }
            data = paginator.page(page).object_list if paginator.page(page).object_list else []
            meta_data = meta if paginator.page(page).object_list else None
            message = UNIT_BUSSINESS if paginator.page(page).object_list else DATA_NOT_FOUND
            return response(rest_status.HTTP_200_OK, data=data, message=message, meta=meta_data, status=True)
        except EmptyPage:
            return response(rest_status.HTTP_200_OK, data=[], message=DATA_NOT_FOUND)

    def is_allowed(self, account_id):
        token = get_token_data(self.request.META, key=None, all=True)
        if token.get('iss') != settings.JWT_ISSUER:
            return True

        account_id_token = token.get('childId') if token.get('childId') else token.get('accountId')
        if account_id != account_id_token:
            return False

        if token.get('iss') != settings.JWT_ISSUER:
            return True

        if token.get('isAdmin'):
            return True

        return False

    def handle_exception(self, exc):
        log.error(exc)

        return response(rest_status.HTTP_404_NOT_FOUND, message='Unit bussiness not found', errors={'item': str(exc)})


@method_decorator(csrf_exempt, name='dispatch')
class SubunitBussinessView(APIView):
    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def post(self, request):
        data = request.data

        # validate user authorization
        account_id = data.get('accountId')
        if not self.is_allowed(account_id):
            return response(rest_status.HTTP_401_UNAUTHORIZED, message='Unauthorize')

        # validate payload and save data to db
        sub_unit = SubunitBussinessSerializers(data=data)
        if not sub_unit.is_valid():
            return response(rest_status.HTTP_400_BAD_REQUEST, message='',  errors=sub_unit.errors)
        sub_unit.save()

        return response(rest_status.HTTP_201_CREATED, sub_unit.data, message="Success", status=True)

    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def put(self, request, id):
        data = request.data

        # validate user authorization
        account_id = data.get('accountId')
        if not self.is_allowed(account_id):
            return response(rest_status.HTTP_401_UNAUTHORIZED, message='Unauthorize')

        # validate payload and update data to db
        sub_unit = SubunitBussinessSerializers(get_object_or_404(Department, id=id), data=data)
        if not sub_unit.is_valid():
            return response(rest_status.HTTP_400_BAD_REQUEST, message='', errors=sub_unit.errors)
        sub_unit.save()

        return response(rest_status.HTTP_201_CREATED, sub_unit.data, message="Success", status=True)

    @partial(loginRequired, module="ACCOUNT", access="VIEW")
    def get(self, request, unit_id='', id=''):
        if id:
            return self.get_by_id(id)

        return self.get_all(unit_id)

    def get_by_id(self, id, message="Subunit bussiness detail"):
        nonstrict()
        department = Department.objects.get(id=id)
        sub_unit = SubunitBussinessSerializers(department).data
        return response(rest_status.HTTP_200_OK, data=sub_unit, message=message, status=True)

    def get_all(self, unit_id):
        serializer = ValidateGetUnitSerializers(data=self.request.GET)
        if not serializer.is_valid():
            return response(rest_status.HTTP_400_BAD_REQUEST, message='Get subunit bussiness list failed',
                            errors=serializer.errors)

        page = serializer.data['page']
        limit = serializer.data['limit']
        search = serializer.data['search']
        account_id = serializer.data['accountId']
        if account_id:
            return self.get_all_by_account(account_id, page, limit)

        query_filter = Q(parent_id=unit_id)
        if search:
            query_filter &= Q(code__icontains=search) | Q(name__icontains=search)

        nonstrict()
        sub_units = Department.objects.filter(query_filter)
        paginator = Paginator(SubunitBussinessSerializers(sub_units, many=True).data, limit)
        meta = {
            "totalRecords": paginator.count,
            "totalPages": paginator.num_pages,
            "page": page,
            "limit": limit
        }

        return response(rest_status.HTTP_200_OK,
                        data=paginator.page(page).object_list,
                        message="Subunit Bussiness",
                        meta=meta,
                        status=True)

    def get_all_by_account(self, account_id, page, limit):
        try:
            token = get_token_data(self.request.META, key=None, all=True)
            if 'accountId' in token and token['accountId'] != account_id:
                return response(rest_status.HTTP_200_OK, data=[], message=DATA_NOT_FOUND)
            units = Department.objects.filter(type='SUBUNIT', account_id=account_id).order_by("name")
            paginator = Paginator(units, limit)
            serializer = SubunitBussinessSerializers(paginator.page(page), many=True)
            meta = {
                "totalRecords": paginator.count,
                "totalPages": paginator.num_pages,
                "page": page,
                "limit": limit
            }
            data = serializer.data if serializer.data else []
            meta_data = meta if serializer.data else None
            message = "Get Sub Unit Bussiness" if serializer.data else DATA_NOT_FOUND
            return response(rest_status.HTTP_200_OK, data=data, message=message, meta=meta_data, status=True)
        except EmptyPage:
            return response(rest_status.HTTP_200_OK, data=[], message=DATA_NOT_FOUND)

    def is_allowed(self, account_id):
        token = get_token_data(self.request.META, key=None, all=True)
        if token.get('iss') != settings.JWT_ISSUER:
            return True

        account_id_token = token.get('childId') if token.get('childId') else token.get('accountId')
        if account_id != account_id_token:
            return False

        if token.get('iss') != settings.JWT_ISSUER:
            return True

        if token.get('isAdmin'):
            return True

        return False

    def handle_exception(self, e):
        log.error(e)
        return response(rest_status.HTTP_400_BAD_REQUEST, message=str(e))


@method_decorator(csrf_exempt, name='dispatch')
class DepartmentView(APIView):
    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def post(self, request, acc_id=''):

        if request.method == 'POST':
            account_id_token = get_token_data(request._request.META, 'accountId')
            super_admin = get_token_data(request._request.META, 'isSuperAdmin')
            is_cms = True if get_token_data(request.META, 'iss') != settings.JWT_ISSUER else False

            # bulk create
            if acc_id != '':
                # check authorized parent account
                check_access = check_access_account(account_id_token=account_id_token, super_admin=super_admin,
                                                    account_id=acc_id, is_cms=is_cms)

                if check_access is False or get_token_data(request._request.META, 'isAdmin') is False:
                    return response(400, message='Unauthorized')

                return bulk_create(request, acc_id)

            # single create
            data = JSONParser().parse(request)
            account_id = setDefaultValue("accountId", data, '')

            # check authorized parent account
            check_access = check_access_account(account_id_token=account_id_token, super_admin=super_admin,
                                                account_id=account_id, is_cms=is_cms)
            if check_access is False:
                return response(400, message='Unauthorized')

            clean_data = structure_json(data)
            validate = validation(clean_data)

            if validate['code'] != 200:
                return response(400, message=validate['message'])

            serializer = departmentSerializers(data=clean_data)

            # start transaction
            sid = transaction.savepoint()
            try:
                if serializer.is_valid(raise_exception=True):
                    serializer.save()

                    message = "Success create department"
                    status = True

                    res = restructure_json(serializer.data)

                    # insert log activity
                    insert_log_activity(log_from='department', old='', new=serializer.data, type='create',
                                        request=request)

                    transaction.savepoint_commit(sid)
                    return response(201, res, message, status)

            except Exception as e:
                transaction.savepoint_rollback(sid)
                log.error(e)
                return response(400, message='Bad Request')

    @partial(loginRequired, module="ACCOUNT", access="VIEW")
    def get(self, request, id=''):
        meta = None
        try:
            token = get_token_data(self.request.META, key=None, all=True)
            is_admin = token['isAdmin'] if 'isAdmin' in token and token['isAdmin'] else False
            is_super_admin = token['isSuperAdmin'] if 'isSuperAdmin' in token and token['isSuperAdmin'] else False
            is_parent = token['isParent'] if 'isParent' in token and token['isParent'] else False

            if id == '':
                all_department = request.GET.get('all', 'false') == 'true'
                if all_department:
                    return  self.get_departments_filter_trx()

                page = int(request.GET.get('page', 1))
                limit = int(request.GET.get('limit', 10))
                search = request.GET.get('search', '')
                account_id = replaceAccountId(request.GET.get('accountId', ''))

                sort_by = request.GET.get('sortBy', 'name')
                sort_order = request.GET.get('sort', 'asc')

                sorting = ""
                if sort_order == "asc":
                    sorting = sort_by
                elif sort_order == "desc":
                    sorting = "-" + sort_by

                keywords = Q(type=Department.TYPE_DEPARTMENT)
                if search != '' or account_id != '':

                    if search:
                        keywords &= (Q(code__icontains=search)| Q(name__icontains=search))

                    if account_id:
                        if (is_parent and is_admin and is_super_admin):
                            if token.get('childId'):
                                keywords &= Q(account_id=token.get('childId'))
                            elif account_id != token['accountId']:
                                keywords &= Q(account_id=account_id, account__parent_id=token['accountId'])
                            else:
                                keywords &= Q(account_id=account_id)
                        else:
                            keywords &= Q(account_id=account_id)

                nonstrict()
                list_department = Department.objects.filter(keywords) \
                    .annotate(
                        total_contact_account_req=Subquery(
                            AccountContact.objects.filter(
                                department_id=OuterRef('pk'), is_disabled=False).values("department_id")
                                .annotate(total=Count("contact")).values("total")[:1], IntegerField()
                        ),
                        total_contact_requestor=Subquery(
                            ContactDepartmentRequestor.objects.filter(
                                department_id=OuterRef('pk')).exclude(
                                contact_id__in=Subquery(
                                AccountContact.objects.filter(
                                    department_id=OuterRef('department_id'),
                                    is_disabled=False
                                ).values_list("contact_id", flat=True)
                                ))
                                .order_by("department").values("department")
                                .annotate(total=Count("department")).values("total")[:1], IntegerField()
                        ),
                        total_contact_approver=Subquery(
                            ContactDepartmentApproval.objects.filter(
                                department_id=OuterRef('pk')
                            ).exclude(contact_id__in=Subquery(
                                AccountContact.objects.filter(
                                    department_id=OuterRef('department_id'),
                                    is_disabled=False
                                ).values_list("contact_id", flat=True))
                            ).order_by("department").values("department")
                            .annotate(total=Count("department")).values("total")[:1], IntegerField()
                        ),
                        total_contact=Case(
                            When(total_contact_approver__isnull=False, total_contact_requestor__isnull=False,
                                 total_contact_account_req__isnull=False,
                                 then=F('total_contact_approver') + F('total_contact_requestor') +
                                      F('total_contact_account_req')),
                            When(total_contact_approver__isnull=False, total_contact_requestor__isnull=False,
                                 total_contact_account_req__isnull=True,
                                 then=F('total_contact_approver') + F('total_contact_requestor')),
                            When(total_contact_approver__isnull=True, total_contact_requestor__isnull=False,
                                 total_contact_account_req__isnull=False,
                                 then=F('total_contact_requestor') + F('total_contact_account_req')),
                            When(total_contact_approver__isnull=False, total_contact_requestor__isnull=True,
                                 total_contact_account_req__isnull=False,
                                 then=F('total_contact_approver') + F('total_contact_account_req')),
                            When(total_contact_approver__isnull=False, total_contact_requestor__isnull=True,
                                 total_contact_account_req__isnull=True,
                                 then=F('total_contact_approver')),
                            When(total_contact_approver__isnull=True, total_contact_requestor__isnull=False,
                                 total_contact_account_req__isnull=True,
                                 then=F('total_contact_requestor')),
                            When(total_contact_approver__isnull=True, total_contact_requestor__isnull=True,
                                 total_contact_account_req__isnull=False,
                                 then=F('total_contact_account_req')),
                            default=F('total_contact_requestor')
                        )
                    ).order_by(sorting)

                paginator = Paginator(list_department, limit)

                res = departmentSerializers(paginator.page(page), many=True)

                meta = {
                    "page": page,
                    "limit": limit,
                    "totalPages": paginator.num_pages,
                    "totalRecords": paginator.count
                }

                clean_data = list(map(lambda item: restructure_json(item), res.data))
            else:
                try:
                    if not is_passed_IDOR_check(token, id):
                        return response(rest_status.HTTP_401_UNAUTHORIZED, message="Unauthorized")
                    nonstrict()
                    detail_department = Department.objects.filter(pk=id).annotate(
                            total_contact_requestor=Subquery(
                                ContactDepartmentRequestor.objects.filter(
                                    department_id=OuterRef('pk')).order_by("department").values("department") \
                                    .annotate(total=Count("department")).values("total")[:1], IntegerField()
                            ),
                            total_contact_approver=Subquery(
                                ContactDepartmentApproval.objects.filter(
                                    department_id=OuterRef('pk')).order_by("department").values("department") \
                                    .annotate(total=Count("department")).values("total")[:1], IntegerField()
                            ),
                            total_contact=F('total_contact_approver') + F('total_contact_requestor')
                        ).first()

                    if not detail_department:
                        return response(400, message="Department doesn't exists")
                except Exception:
                    detail_department = None
                    return response(404, message="Department doesn't exists")

                serializer_department = departmentSerializers(detail_department)
                clean_data = restructure_json(serializer_department.data, id = id)

            if clean_data:
                message = "Get Department" if id == '' else "Get Department Detail"
                status = True

                return response(200, clean_data, message, status, meta)

            else:
                return response(200, data=[], message='data not found', status=True)

        except Exception as e:
            return response(400, message=str(e))

    def get_departments_filter_trx(self):
        data = self.request.GET
        err, account_id, contact_id = self.get_account_contact(data.get('accountId'))
        if err:
            return response(rest_status.HTTP_400_BAD_REQUEST, message=err)

        departments = Department.objects.origin_query().filter(account_id=account_id)
        if not contact_id:
            result = self.map_department_filter_trx(departments)
            return response(rest_status.HTTP_200_OK, data=result, message='success', status=True)

        dept_app = ContactDepartmentApproval.objects.filter(contact_id=contact_id)
        dept_app_ids = list(dept_app.values_list("department_id", flat=True))
        dept_req = ContactDepartmentRequestor.objects.filter(contact_id=contact_id)
        dept_req_ids = list(dept_req.values_list("department_id", flat=True))
        deptartment_ids = list(set(dept_app_ids+dept_req_ids))
        departments = departments.filter(id__in=deptartment_ids)

        result = self.map_department_filter_trx(departments)
        return response(rest_status.HTTP_200_OK, data=result, message='success', status=True)

    def map_department_filter_trx(self, departments):
        return list(map(lambda i: {
            'id': i.id,
            'accountId': i.account_id,
            'code': i.code,
            'name': i.name,
            'approvalNumber': 0,
            'priceLimit': 0,
            'shoppingLimit': 0,
            'budgetRemaining': 0,
            'totalContact': 0,
            'totalUnit': 0,
            'totalSubunit': 0,
            'lastUpdateBudget': 0,
        }, departments))

    def get_account_contact(self, account_id):
        token = get_token_data(self.request.META, key=None, all=True)
        if token.get('iss') != settings.JWT_ISSUER:
            return None, account_id, None

        token_account_id = token.get('childId') or token.get('accountId')
        if token_account_id != account_id:
            return "You are not allowed", None, None

        if token.get('isAdmin') or token.get('isSuperAdmin'):
            return None, account_id, None

        contact_id = token.get('contactId')
        if not contact_id:
            return 'Contact not found', None, None

        return None, account_id, token.get('contactId')

    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def put(self, request, id=''):
        token = get_token_data(self.request.META, key=None, all=True)
        data = JSONParser().parse(request)
        if not is_passed_IDOR_check(token, id):
            return response(rest_status.HTTP_401_UNAUTHORIZED, message="Unauthorized")
        try:
            nonstrict()
            department = Department.objects.get(pk=id)
        except Exception as e:
            return response(400, message=str(e))

        get_department = departmentSerializers(department)
        dp = department.approval_number
        clean_data = structure_json(data)
        validate = validation(clean_data, pk=id)

        if validate['code'] != 200:
            return response(400, message=validate['message'])

        serializer = departmentSerializers(department, data=clean_data)

        # start transaction
        sid = transaction.savepoint()
        try:
            department = Department.objects.origin_query().get(pk=id)

            get_department = departmentSerializers(department)
            dp = department.approval_number
            clean_data = structure_json(data)
            validate = validation(clean_data, pk=id)

            if validate['code'] != 200:
                return response(400, message=validate['message'])

            serializer = departmentSerializers(department, data=clean_data)

            # start transaction
            if not serializer.is_valid():
                transaction.savepoint_rollback(sid)
                return response(400, message="Failed update department", errors=serializer.errors, extract_message=True)

            if 'order' in data:
                err = update_order(data, dp, department.id)
                if err:
                    transaction.savepoint_rollback(sid)
                    return response(400, message="Failed update department. {}".format(err))
            serializer.save()
            res = restructure_json(serializer.data)
            insert_log_activity(log_from='department', old=get_department.data, new=serializer.data, type='update',
                                request=request)
            transaction.savepoint_commit(sid)

            return response(201, res, message="Success update department", status=True)
        except Exception as e:
            transaction.savepoint_rollback(sid)
            return response(400, message=str(e))

    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def delete(self, request, id='', acc_id=''):
        if 'contact' in request.path:
            return remove_account(request, id, acc_id)

        return response(200, message="no action")

def is_passed_IDOR_check(token, id=''):
    # Penjagaan IDOR Attack by DB Checking
    if not token.get('iss') == settings.JWT_ISSUER:
        return True

    search_contact = Q(id=token['contactId'], email=token['email'], is_disabled=False)
    is_valid_contact = Contact.objects.origin_query().filter(search_contact).first()
    if not is_valid_contact:
        return False

    account_contact = AccountContact.objects.origin_query().filter(contact_id=is_valid_contact.id, is_delete=False).values('account_id', 'is_admin')
    accounts = [x.get('account_id') for x in account_contact]
    is_admin = [x.get('is_admin') for x in account_contact]

    if not accounts:
        return False

    if True in is_admin:
        return True

    dbdata = Department.objects.filter(account_id__in=accounts).values('id')
    dept_list = [data.get('id') for data in dbdata]

    if int(id) not in dept_list:
        return False
    return True

def update_order(data, approval_number, id):
    # nilai `old` dan `new` tidak boleh 0
    # nilai `old` tidak boleh melebihi `approvalNumber` existing
    # nilai `new` tidak boleh melebihi `approvalNumber` di payload
    # jumlah item `order` harus sama dengan `approvalNumber` existing
    invalid_zero_level = len(list(filter(lambda d: d["old"] == 0 or d["new"] == 0, data["order"])))
    invalid_old_level = len(list(filter(lambda d: d["old"] > approval_number, data["order"])))
    new_is_more_than_approval_numb = len(list(filter(lambda d: d["new"] > int(data["approvalNumber"]), data["order"])))
    is_order_count_is_invalid = len(data["order"]) > approval_number

    if invalid_old_level > 0:
        return "the old level should not exceed the maximum approval number"
    if invalid_zero_level > 0:
        return "cannot insert 0 value in old and new level"
    if new_is_more_than_approval_numb > 0:
        return "new level cannot more than approval number inserted"
    if is_order_count_is_invalid:
        return "level length doesn't match with approval number"
    
    approvals_by_department = ContactDepartmentApproval.objects.filter(department_id=id)
    if data["approvalNumber"] == 0:
        approvals_by_department.delete()
    else:    
        for order in data["order"]:
            approvals_by_department.filter(order=order["old"]).update(order=order["new"])

    return None


@method_decorator(csrf_exempt, name='dispatch')
class SubDepartmentContactView(APIView):
    @partial(loginRequired, module="ACCOUNT", access="VIEW")
    def get(self, request, dept_id=0):
        try:
            search = request.GET.get('search', '')

            try:
                nonstrict()
                detail_department = Department.objects\
                    .annotate(
                        total_contact_requestor=Subquery(
                            ContactDepartmentRequestor.objects.filter(
                                department_id=OuterRef('pk')).order_by("department").values("department") \
                                .annotate(total=Count("department")).values("total")[:1], IntegerField()
                        ),
                        total_contact_approver=Subquery(
                            ContactDepartmentApproval.objects.filter(
                                department_id=OuterRef('pk')).order_by("department").values("department") \
                                .annotate(total=Count("department")).values("total")[:1], IntegerField()
                        ),
                        total_contact=F('total_contact_approver') + F('total_contact_requestor')
                    )\
                    .get(pk=dept_id)
            except Department.DoesNotExist:
                return response(404, message="Subdepartment doesn't exists")

            # finalizing data
            serializer_department = departmentSerializers(detail_department)
            clean_data = restructure_json_nonapprover(item=serializer_department.data, id=dept_id, search=search)
            if clean_data:
                limit_rows = 20
                total_rows = len(clean_data)
                meta = {
                    "totalRecords": total_rows,
                    "totalPages": math.ceil(total_rows / limit_rows),
                    "page": 1,
                    "limit": limit_rows
                }

                return response(200, data=clean_data, message="Get Subdepartment contact list", status=True, meta=meta)
            else:
                return response(200, data=[], message='Data not found', status=True)

        except Exception as e:
            return response(400, message=str(e))


@method_decorator(csrf_exempt, name='dispatch')
class ContactInvite(APIView):
    def post(self, request, id='', *args, **kwargs):
        return contact_invitation(request, id)


@method_decorator(csrf_exempt, name='dispatch')
class ContactInviteDepartment(APIView):
    def post(self, request, acc_id='', *args, **kwargs):
        return contact_invite_department(request, acc_id)


@method_decorator(csrf_exempt, name='dispatch')
class DepartmentInviteContact(APIView):
    def post(self, request, id_department='', *args, **kwargs):
        return department_invite_contact(request, id_department)


def department_invite_contact(request, id_department):
    account_id_token = get_token_data(request._request.META, 'accountId')
    super_admin = get_token_data(request._request.META, 'isSuperAdmin')
    data = JSONParser().parse(request)
    data['departmentId'] = id_department
    account_id = setDefaultValue("accountId", data, '')

    # user cms
    is_cms = True if get_token_data(request.META, 'iss') != settings.JWT_ISSUER else False

    # check authorized parent account
    check_access = check_access_account(account_id_token=account_id_token, super_admin=super_admin,
                                        account_id=account_id, is_cms=is_cms)

    if check_access is False or get_token_data(request._request.META, 'isAdmin') is False:
        return response(400, message='Unauthorized')

    sid = transaction.savepoint()
    res_data = []
    contact_id_requestor = []
    error = False
    res = response(400, message='Invite requestor failed')
    try:
        # check department exist
        nonstrict()
        department = Department.objects.get(id=data['departmentId'], account=account_id)

        temp_approver_exist = []
        approver = ContactDepartmentApproval.objects.filter(department_id=data['departmentId']).distinct()
        if approver.count() > 0:
            for approver in approver:
                temp_approver_exist.append(approver.contact_id)

        temp_contact_id = []
        requestor = ContactDepartmentRequestor.objects.filter(department_id=data['departmentId']).distinct()
        if requestor.count() > 0:
            for requestor in requestor:
                temp_contact_id.append(requestor.contact_id)

        set_requester_new_value = list(set(data['requestor']) - set(temp_contact_id))
        set_approval_new_value = approver_new_value(data, temp_approver_exist)

        if department:
            contact_id_list = data['requestor']
            if len(contact_id_list) != len(set(contact_id_list)):
                return response(400, message="Contact duplicate from your requestor list")

            # check total email list, max 20
            if len(contact_id_list) > 20:
                transaction.savepoint_rollback(sid)
                return response(400, message="Maximum requestor email is 20")
            # check total email less then 1
            elif len(contact_id_list) < 1:
                transaction.savepoint_rollback(sid)
                return response(400, message="Minimum requestor contact is 1")
            else:
                # if email already in contact
                res = ""
                for contact_id in contact_id_list:
                    contact_list = AccContactSerializer(
                        AccountContact.objects.filter(contact=contact_id, account_id=account_id, is_disabled=False),
                        many=True).data

                    if len(contact_list) == 0:
                        error = True
                        res = response(404,
                                       message="Contact not found with account %s, "
                                               "please make sure the chosen contact is related "
                                               "with selected account" % (account_id))
                        break

                    for con_id in contact_list:
                        if con_id['contact'] in set(temp_approver_exist):
                            error = True
                            res = response(400, message="Contact from your requestor list already used as approver")
                            break

                        clean_data_bulk = structure_json_requestor(department.id, con_id['contact'])
                        serializer_requestor = ContactDeptRequestorSerializer(data=clean_data_bulk)
                        if not serializer_requestor.is_valid():
                            res = response(400, message=serializer_requestor.errors)
                            error = True
                            break

                        contact_id_requestor.append(con_id['contact'])
                        temp_contact_id.append(con_id['contact'])
                        serializer_requestor.update_or_create()

            if error:
                transaction.savepoint_rollback(sid)
                return res

            # set approver
            approver_list = []
            if department.approval_number:
                if department.approval_number != len(data['approver']):
                    transaction.savepoint_rollback(sid)
                    return response(400, message='{0}, The order number must be the same as '
                                                 'the number of approvals in department'.format(department.name))
                else:
                    temp_contact_id_approver = []
                    for cont in data['approver']:
                        list_contact_id_temp = cont['contactId']
                        for con_id in list_contact_id_temp:
                            if con_id in set(temp_contact_id):
                                transaction.savepoint_rollback(sid)
                                return response(400, message="Contact from your approver list already used as requestor")
                            if con_id in set(temp_contact_id_approver):
                                transaction.savepoint_rollback(sid)
                                return response(400, message="Contact duplicate from your approver list")

                            temp_contact_id_approver.append(con_id)

                    for cont in data['approver']:
                        approver_id = []
                        if len(cont) != 0:
                            list_contact_id = cont['contactId']
                            if len(list_contact_id) == 0:
                                res = response(400, message='Minimum approval contact is 1')
                                error = True
                                break

                            if len(list_contact_id) != len(set(list_contact_id)):
                                res = response(400, message="Contact duplicate from your approver list")
                                error = True
                                break

                            order = cont['order']
                            if (department.approval_number < order) or (order < 1):
                                res = response(400, message='{0}, order must be greater than 0 and less equal '
                                                            'to max value approval number in department'.format(department.name))
                                error = True
                                break

                            for con_id in list_contact_id:
                                AccountContact.objects.get(contact_id=con_id, account_id=account_id)
                                clean_data_bulk_app = structure_json_approval(department.id, con_id, order)
                                serializer_approver = ContactDeptApprovalSerializer(data=clean_data_bulk_app)

                                if not serializer_approver.is_valid():
                                    res = response(400, message=serializer_approver.errors)
                                    error = True
                                    break

                                approver_id.append(con_id)
                                serializer_approver.update_or_create()

                            approver_list.append({"contactId": approver_id, "order": order})

            item_res = {"id": id_department, "approver": approver_list, "requestor": contact_id_requestor}
            res_data.append(item_res)

            if not error:
                # insert log activity
                insert_log_activity(log_from='department', old='', new=res_data[0], type='assign',
                                    request=request)

                for contact_id in (set_requester_new_value + set_approval_new_value):
                    member_type = department.account.member_type
                    send_invitation_department(contact_id, department.name, member_type=member_type)

                res = response(201, data=res_data, message="Assign requestor & approval success", status=True)

    except Department.DoesNotExist:
        transaction.savepoint_rollback(sid)
        res = response(404,
                       message="Department not found with account %s, please make sure the chosen departments "
                               "is related with selected account" % (account_id))
        return res

    except AccountContact.DoesNotExist:
        transaction.savepoint_rollback(sid)
        res = response(404,
                       message="Contact not found with account %s, please make sure the chosen contact "
                               "is related with selected account" % (account_id))
        return res

    except Exception as e:
        transaction.savepoint_rollback(sid)
        log.error(e)
        return response(400, message=str(e))

    return res


def approver_new_value(request_data, current_approver):
    approver_data = []
    try:
        for approver in request_data['approver']:
            approver_data = approver_data + approver['contactId']

        approver_list = list(set(approver_data) - set(current_approver))
    except Exception as e:
        log.error(e)
        return []

    return approver_list


def send_invitation_department(contact_id, department_name, member_type=''):
    if contact_id == 0:
        return None
    try:
        data_contact = ContactSerializers(Contact.objects.get(pk=contact_id)).data

        label, from_email, _, bisnis_label = get_label(member_type=member_type)

        mail = {
            "subject": f"Anda Telah Diundang Untuk Bergabung Sebagai Pengguna {bisnis_label}",
            "from_email": from_email,
            "to": data_contact['email']
        }

        content = {
            "headerImage": settings.HEADER_IMG,
            "headerImageBg": settings.HEADER_IMG_BG,
            "link": settings.HOST_SHARK_CF,
            "department": department_name,
            "name": data_contact['email'],
            "tanggal": datetime.now().strftime("%d/%m/%Y"),
            "pukul": datetime.now().strftime("%H:%M:%S"),
            "label": label,
            "isCorporate": label == settings.ARONAWA_LABEL
        }
    except Exception as e:
        log.error(e)
        return None

    template = 'email_invitation_department.html'

    return send(mail, content, template)


def role_approver_department(request, acc_id):
    data = JSONParser().parse(request)
    res = response(400, "no action")
    sid = transaction.savepoint()
    res_data = []

    try:
        if 'department' not in data or len(data['department']) == 0:
            res = response(400, message='invalid department data')

        else:
            error = False

            for data_approver in data['department']:
                department_id = data_approver['id']
                for cont in data_approver['approver']:
                    if len(cont) != 0:
                        list_contact_id = cont['contactId']
                        if len(list_contact_id) == 0 :
                            res = response(400, message='contactId can not be blank')
                            error = True
                            break

                        order = cont['order']
                        department = Department.objects.get(pk=department_id, account_id=acc_id)
                        if (department.approval_number < order) or (order < 1):
                            res = response(400, message='{0}, order must be greater than 0 and less equal to max value approval number in department'.format(department.name))
                            error = True
                            break

                        for con_id in list_contact_id:
                            AccountContact.objects.get(contact_id=con_id, account_id= acc_id)
                            clean_data_bulk_app = structure_json_approval(department_id, con_id, order)
                            serializer = ContactDeptApprovalSerializer(data=clean_data_bulk_app)

                            if not serializer.is_valid():
                                res = response(400, message=serializer.errors)
                                error = True
                                break

                            serializer.update_or_create()

                        item_res = {
                            "departmentName": department.name,
                            "order": order
                        }
                        res_data.append(item_res)

            if not error:
                res = response(201, data = res_data, message="approval created", status=True)

    except Department.DoesNotExist:
        res = response(400, message="Department not found with account %s, please make sure the chosen departments is related with selected account" % (
                        acc_id))
        transaction.savepoint_rollback(sid)

    except AccountContact.DoesNotExist:
        res = response(400,
                       message="Contact not found with account %s, please make sure the chosen contact is related with selected account" % (
                           acc_id))
        transaction.savepoint_rollback(sid)

    except Exception as e:
        log.error(e)
        transaction.savepoint_rollback(sid)
        res = response(400, message="Cannot create approval")

    return res


@csrf_exempt
def remove_account(request, id, acc_id):
    try:
        account_id_token = get_token_data(request._request.META, 'accountId')
        super_admin = get_token_data(request._request.META, 'isSuperAdmin')
        acc = AccountContact.objects.get(contact_id=acc_id, account__department__id=id)
        # check authorized parent account
        is_cms = True if get_token_data(request.META, 'iss') != settings.JWT_ISSUER else False
        check_access = check_access_account(account_id_token=account_id_token,
                                            super_admin=super_admin,
                                            account_id=acc.account_id,
                                            is_cms=is_cms)

        if check_access is False or get_token_data(request._request.META, 'isAdmin') is False:
            return response(400, message='Unauthorized')

        department_requestor = ContactDepartmentRequestor.objects.filter(contact_id=acc_id, department_id=id)
        department_approval = ContactDepartmentApproval.objects.filter(contact_id=acc_id, department_id=id)
        if len(department_requestor) == 0 and len(department_approval) == 0 and acc.department_id != int(id):
            return response(404, message="Contact is not listed in department")

        if len(department_requestor) > 0:
            # check transaction ongoing cannot delete user dept, only (done/canceled)
            total_open_opty = Opportunity.objects.origin_query().filter(account_id=acc.account_id, contact_id=acc_id,
                                                         department_id=id).exclude(Q(stage_id="CLOSED_WIN") |
                                                                                   Q(stage_id="CANCELED_BY_REQUESTOR") |
                                                                                   Q(is_delete=True)).count()
            if total_open_opty >= 1:
                return response(400, message='The contact is currently ongoing transaction')

            old_department_requestor = ContactDeptRequestorSerializer(department_requestor.first()).data
            # remove contact & department in table contact_department_requestor
            department_requestor.delete()
            # insert log activity
            insert_log_activity(log_from='department-requestor', old=old_department_requestor, new='', type='delete',
                                request=request)

        if len(department_approval) > 0:
            # check transaction ongoing cannot delete user dept, only (done/canceled)
            get_quot = TransactionApproval.objects.filter(user_id=acc_id).first()
            if get_quot:
                get_opty = Quotation.objects.origin_query().filter(id=get_quot.quotation_id).first()
                if get_opty:
                    total_open_opty = Opportunity.objects.origin_query().filter(
                        account_id=acc.account_id, id=get_opty.opportunity_id, department_id=id
                    ).exclude(Q(stage_id="CLOSED_WIN") | Q(stage_id="CANCELED_BY_REQUESTOR") | Q(is_delete=True)).count()
                    if total_open_opty > 0:
                        return response(400, message='The contact is currently ongoing transaction')

            old_department_approval = ContactDeptApprovalSerializer(department_approval.get()).data
            # remove contact & department in table contact_department_approval
            department_approval.delete()
            # insert log activity
            insert_log_activity(log_from='department-approval', old=old_department_approval, new='', type='delete',
                                request=request)

        # remove department in table accountContact
        acc.department_id = None
        acc.save()

        return response(200, message="Contact successfully deleted from department", status=True)
    except AccountContact.DoesNotExist:
        return response(404, message="Contact not found")
    except Exception as e:
        log.error(e)
        return response(400, message='Bad Request')


@csrf_exempt
def contact_invitation(request, id):
    data = JSONParser().parse(request)
    status = True
    data_res = []
    status_code = 201

    department_exist = Department.objects.filter(id=id).count()
    if department_exist == 0:
        return response(400, message="Department doesn't exist")

    account_id = setDefaultValue("accountId", data, '')
    if account_id == '':
        return response(400, message="Field accountId is required")

    email_list = setDefaultValue("email", data, [])
    if email_list == []:
        return response(400, message="Field email should contain at least one email")

    account_exist = Account.objects.filter(pk=account_id, is_disabled=False, is_delete=False)
    if not account_exist:
        return response(400, message="Account doesn't exist")

    if len(email_list) > 20:
        return response(400, message="Max email is 20")

    member_type = account_exist.first().member_type

    for email in email_list:
        # start transaction
        sid = transaction.savepoint()

        format_email = validate_email(email)
        if format_email is False:
            message = "Format email is wrong"
            status = False
            data_res.append(invitationView.response_json(email, status, message))
        else:
            contact = Contact.objects.filter(email=email, is_disabled=False)

            if len(contact) > 0:
                contact_ids = list(contact.values_list("id", flat=True))
                acc_contact = AccountContact.objects.filter(account=account_id, contact_id__in=contact_ids, department=id, is_disabled=False)

                if len(acc_contact) > 0:
                    message = "Already in choosen department"
                    status = False
                    data_res.append(invitationView.response_json(email, status, message))

                else:
                    is_already_sent = False
                    account_contact_exist = AccountContact.objects.filter(account=account_id, contact_id__in=contact_ids, is_disabled=False)
                    account_contact = account_contact_exist if account_contact_exist > 0 \
                        else AccountContact.objects.filter(contact_id__in=contact_ids, is_disabled=False)
                    
                    contact_list = AccContactSerializer(
                        account_contact,
                        many=True).data

                    for item in contact_list:
                        if is_already_sent == True:
                            continue

                        if item['account'] == account_id and item['department'] != id:
                            current_department = '-'

                            if item['department']:
                                current_department = departmentSerializers(Department.objects.get(pk=item['department'])).data['name']

                            email_data = {
                                "lastDepartment": current_department,
                                "newDepartment": departmentSerializers(Department.objects.get(pk=id)).data['name'],
                                "email": email,
                                "memberType": member_type
                            }

                            AccountContact.objects.filter(id=item['id'], is_disabled=False).update(department=id)
                            send_email_change_department(email_data)
                            data_res.append(invitationView.responseJson(email, status, message=SUCCESS_INVITE_DEPT))
                            is_already_sent = True

                        elif item['account'] != account_id:
                            inv_data = structure_invitation(item, email)
                            clean_data = invitationView.structure_json(inv_data)
                            serializer = invitationSerializers(data=clean_data)

                            try:
                                if serializer.is_valid(raise_exception=True):
                                    # todo token narwhal ngga ada userid done
                                    serializer.validated_data['created_by'] = get_user_id(
                                        get_token_data(request._request.META, 'email'))
                                    serializer.save()

                                    invitationView.sent_invitation(request, serializer.data, member_type=member_type)
                                    data_res.append(invitationView.responseJson(email, status, message=SUCCESS_INVITE_DEPT))
                                    is_already_sent = True

                            except Exception as e:
                                data_res.append(invitationView.response_json(email, status=False, message=str(e)))
                                return response(400, data_res, " ", status=False)
            else:
                inv_data = {
                    "accountId": account_id,
                    "departmentId": id,
                    "email": email,
                }
                clean_data = invitationView.structure_json(inv_data)
                serializer = invitationSerializers(data=clean_data)

                try:
                    if serializer.is_valid(raise_exception=True):
                        # todo token narwhal ngga ada userid done
                        serializer.validated_data['created_by'] = get_user_id(
                            get_token_data(request._request.META, 'email'))
                        
                        serializer.save()
                        invitationView.sent_invitation(request, serializer.data, member_type=member_type)
                        data_res.append(invitationView.response_json(email, status, message=SUCCESS_INVITE_DEPT))

                except Exception as e:
                    data_res.append(invitationView.response_json(email, status=False, message=str(e)))
                    return response(400, data_res, " ", status=False)

    transaction.savepoint_commit(sid)
    return response(status_code, data_res, " ", status)


def contact_invite_department(request, acc_id):
    try:
        Account.objects.get(pk=acc_id)
    except Account.DoesNotExist:
        return response(404, message="Account doesn't exists")

    try:
        data = JSONParser().parse(request)
        validate_data = list(map(lambda item: validation_bulk_contact_invite(item, acc_id, request), data))
        filter_validate_data = list(filter(lambda x: x['status'] == True, validate_data))

        if len(filter_validate_data) == 0:
            return response(400, message=validate_data)
        else:
            return response(201, message=validate_data, status=True)

    except Exception as e:
        return response(400, message=str(e))


def send_email_change_department(data):
    label, from_email, corporate_name, bisnis_label = get_label(data['memberType'])
    mail = {
        "subject": "Anda Telah Diundang Untuk Bergabung Sebagai Pengguna {}".format(bisnis_label),
        "from_email": from_email,
        "to": data['email']
    }

    content = {
        "headerImage": settings.HEADER_IMG,
        "headerImageBg": settings.HEADER_IMG_BG,
        "link": settings.LINK_LOGIN,
        "lastDepartment": data['lastDepartment'],
        "newDepartment": data['newDepartment'],
        "name": data['email'],
        "tanggal": datetime.now().strftime("%d/%m/%Y"),
        "pukul": datetime.now().strftime("%H:%M:%S"),
        "label": label,
        "corporateName": corporate_name,
        "bisnisLabel": bisnis_label,
        "isCorporate": label == settings.ARONAWA_LABEL
    }

    template = 'email_change_department.html'

    return send(mail, content, template)


def structure_invitation(item, email=''):
    data = {
        "accountId": setDefaultValue('account', item, ''),
        "contactId": setDefaultValue('contact', item, None),
        "departementId": setDefaultValue('department', item, None),
        "email": email
    }

    return data


def structure_json(item):
    data = {
        "account": setDefaultValue('accountId', item, ''),
        "code": setDefaultValue('code', item, '').upper(),
        "name": setDefaultValue('name', item, ''),
        "approval_number": setDefaultValue('approvalNumber', item, ''),
        "price_limit": setDefaultValue('priceLimit', item, ''),
        "shopping_limit": setDefaultValue('shoppingLimit', item, ''),
        "type": Department.TYPE_DEPARTMENT,
    }
    if 'budget' in item:
        data['budget'] = item['budget']

    return data


def restructure_json(item, id=None):
    nonstrict()
    units = Department.objects.filter(parent_id=item['id'])
    total_unit = units.count()
    total_subunit = 0
    for unit in units:
        nonstrict()
        total_subunit += Department.objects.filter(parent_id=unit.id).count()
    id = setDefaultValue('id', item, '')
    dept_id = id
    parent_id = setDefaultValue('parent_id', item, id)
    if parent_id:
        # Check for unit
        dept_data = Department.objects.filter(id=id).first()
        if dept_data.type == 'UNIT':
            dept_id = dept_data.parent.id
        # Check for subunit
        elif dept_data.type == 'SUBUNIT':
            dept_id = dept_data.parent.parent.id
    data = {
        "id": id,
        "accountId": setDefaultValue('account', item, ''),
        "code": setDefaultValue('code', item, ''),
        "name": setDefaultValue('name', item, ''),
        "approvalNumber": setDefaultValue('approval_number', item, ''),
        "priceLimit": setDefaultValue('price_limit', item, ''),
        "shoppingLimit": setDefaultValue('shopping_limit', item, ''),
        "budgetRemaining": getBudgetRemaining(setDefaultValue('id', item, None)),
        "totalContact": setDefaultValue('total_contact', item, 0),
        "totalUnit": total_unit,
        "totalSubunit": total_subunit,
        "lastUpdateBudget": get_last_budget_history(item['id']),
        "departmentId": dept_id
    }

    if id:
        list_contact_id = []

        # approver contact list
        approver_list = []
        approver_number = setDefaultValue('approval_number', item, 0)
        for order_number in range(approver_number):
            order_number += 1
            department_approver = ContactDepartmentApproval.objects.filter(department_id=id, order=order_number).distinct().order_by("order")
            approver_id = []
            if len(department_approver) > 0:
                for approver in department_approver:
                    list_contact_id.append(approver.contact_id)
                    contact_detail = Contact.objects.get(id=approver.contact_id)
                    approver_id.append({
                        "id": contact_detail.id,
                        "firstName": contact_detail.first_name,
                        "lastName": contact_detail.last_name,
                        "salutation": contact_detail.salutation,
                        "email": contact_detail.email
                    })

                approver_list.append({"contactId": approver_id, "order": order_number})

        # requestor contact list
        account_contact_list = AccountContact.objects.filter(account=data['accountId'], is_disabled=False)
        requestor_list = []
        department_requestor = ContactDepartmentRequestor.objects.filter(department_id=id).distinct()
        if len(department_requestor) > 0:
            for requestor in department_requestor:
                list_contact_id.append(requestor.contact_id)
                contact_detail = Contact.objects.filter(id=requestor.contact_id).first()
                if contact_detail:
                    requestor_list.append({
                        "id": contact_detail.id,
                        "firstName": contact_detail.first_name,
                        "lastName": contact_detail.last_name,
                        "salutation": contact_detail.salutation,
                        "email": contact_detail.email
                    })

        account_contact_exist = account_contact_list.filter(department_id=id).exclude(contact_id__in=list_contact_id)

        if len(account_contact_exist) > 0:
            for contact_exist in account_contact_exist:
                list_contact_id.append(contact_exist.contact_id)
                contact_detail = Contact.objects.filter(id=contact_exist.contact_id).first()
                if contact_detail:
                    requestor_list.append({
                        "id": contact_detail.id,
                        "firstName": contact_detail.first_name,
                        "lastName": contact_detail.last_name,
                        "salutation": contact_detail.salutation,
                        "email": contact_detail.email
                    })

        # user contact list
        email_list = []
        data['accountId'] = item['account_id'] if data['accountId'] == '' else data['accountId']
        contact_ids = account_contact_list.filter(
            status__in=[Account.ACTIVATED, AccountContact.STATUS_REGISTERED]).exclude(
            contact_id__in=list_contact_id).values_list("contact_id", flat=True)
        contact_detail_list = Contact.objects.filter(id__in=contact_ids)
        for user_list in contact_detail_list:
            email_list.append({
                "id": user_list.id,
                "firstName": user_list.first_name,
                "lastName": user_list.last_name,
                "salutation": user_list.salutation,
                "email": user_list.email
            })

        total_contact = len(approver_list) + len(requestor_list)
        data.update({
            "user": email_list,
            "approver": approver_list,
            "requestor": requestor_list,
            "totalContact": total_contact
        })

    return data

def restructures_json(item, id=None):
    department_parent = ""
    unit_parent = ""
    if str(item['type']).lower() == "subunit":
        nonstrict()
        unit = Department.objects.filter(id=item['parent_id']).first()
        unit_parent = unit.name
        nonstrict()
        department = Department.objects.filter(id=unit.parent_id).first()
        department_parent = department.name
    elif str(item['type']).lower() == "unit":
        nonstrict()
        department = Department.objects.filter(id=item['parent_id']).first()
        department_parent = department.name

    data = {
        "id": setDefaultValue('id', item, ''),
        "accountId": setDefaultValue('account_id', item, ''),
        "code": setDefaultValue('code', item, ''),
        "name": setDefaultValue('name', item, ''),
        "approvalNumber": setDefaultValue('approval_number', item, ''),
        "priceLimit": setDefaultValue('price_limit', item, ''),
        "shoppingLimit": setDefaultValue('shopping_limit', item, ''),
        "budgetRemaining": getBudgetRemaining(setDefaultValue('id', item, None)),
        "totalContact": setDefaultValue('total_contact', item, 0),
        "lastUpdateBudget": get_last_budget_history(item['id']),
        "type": item['type'],
        "unit": unit_parent,
        "department": department_parent
    }
    return data

def restructure_json_nonapprover(item, id, search):
    list_contact_id = []
    account_id = setDefaultValue('account', item, '')

    # approver list of contact id
    approver_number = setDefaultValue('approval_number', item, 0)
    for order_number in range(approver_number):
        order_number += 1
        department_approver = ContactDepartmentApproval.objects.\
            filter(department_id=id, order=order_number).\
            order_by("order")
        for approver in department_approver:
            list_contact_id.append(approver.contact_id)

    # requestor contact list
    requestor_list = []
    department_requestor = ContactDepartmentRequestor.objects.\
        filter(department_id=id)
    for requestor in department_requestor:
        list_contact_id.append(requestor.contact_id)
        contact_detail = Contact.objects.filter(
            Q(id=requestor.contact_id),
            Q(first_name__icontains=search) | Q(last_name__icontains=search) | Q(email__icontains=search)
        ).first()
        if contact_detail is None: continue
        requestor_list.append({
            "id": contact_detail.id,
            "firstName": contact_detail.first_name,
            "lastName": contact_detail.last_name,
            "salutation": contact_detail.salutation,
            "email": contact_detail.email
        })

    account_contact_exist = AccountContact.objects.\
        filter(account=account_id, department_id=id, is_disabled=False).\
        exclude(contact_id__in=list_contact_id)
    for contact_exist in account_contact_exist:
        list_contact_id.append(contact_exist.contact_id)
        contact_detail = Contact.objects.filter(
            Q(id=contact_exist.contact_id),
            Q(first_name__icontains=search) | Q(last_name__icontains=search) | Q(email__icontains=search)
        ).first()
        if contact_detail is None: continue
        requestor_list.append({
            "id": contact_detail.id,
            "firstName": contact_detail.first_name,
            "lastName": contact_detail.last_name,
            "salutation": contact_detail.salutation,
            "email": contact_detail.email
        })

    # user contact list
    email_list = []
    account_contact = AccountContact.objects.filter(status__in=[Account.ACTIVATED, AccountContact.STATUS_REGISTERED],
                                                    account=account_id, is_disabled=False).exclude(
        contact_id__in=list_contact_id)
    contact_list = account_contact.values_list("contact_id", flat=True)
    contact_detail_list = Contact.objects.filter(
        Q(id__in=contact_list),
        Q(first_name__icontains=search) | Q(last_name__icontains=search) | Q(email__icontains=search)
    )
    for user_list in contact_detail_list:
        email_list.append({
            "id": user_list.id,
            "firstName": user_list.first_name,
            "lastName": user_list.last_name,
            "salutation": user_list.salutation,
            "email": user_list.email
        })

    # finalizing data result
    result = (email_list + requestor_list)

    return result


def validation(data, pk=None):
    res = {
        "code": 200,
        "message": "Success"
    }

    try:
        # validation code
        if data['code']:
            if pk:
                total_department = Department.objects.filter(account=data['account'], code=data['code']).exclude(pk=pk).count()
            else:
                total_department = Department.objects.filter(account=data['account'], code=data['code']).count()
            if total_department > 0:
                res = {
                    "code": 400,
                    "message": "Code already exist."
                }

        if int(data['price_limit']) > int(data['shopping_limit']):
            res = {
                "code": 400,
                "message": "Shopping limit must be greater than price limit"
            }

        # check limit
        if ('budget' in data) and ((data['price_limit'] > data['budget']) or (data['shopping_limit'] > data['budget'])):
            res = {
                "code": 400,
                "message": "Budget must be greater than Shopping limit or price limit "
            }

        return res

    except Exception as e:
        return {
            "code": 400,
            "message": str(e)
        }


def bulk_create(request, acc_id):
    data = JSONParser().parse(request)
    list(map(lambda item: item.update({'accountId': acc_id}), data))
    # extract code
    ex_code = [d['code'] for d in data]
    dup_code = {i: ex_code.count(i) for i in ex_code}

    # get max duplicate
    max_code = max({ex_code.count(i) for i in ex_code})

    if max_code > 1:
        list_duplicate = []
        for dt in data:
            if dt['code'] in dup_code:
                if dup_code[dt['code']] > 1:
                    item_duplicate = {
                        "codeMessage": 400,
                        "code": "Duplicate code : " + dt['code']
                    }
                else:
                    item_duplicate = {
                        "codeMessage": 200,
                        "code": dt['code']
                    }

                list_duplicate.append(item_duplicate)

        return response(400, message=list_duplicate)

    try:
        Account.objects.get(pk=acc_id)
    except Exception:
        return response(400, message="Account not found")

    clean_data = list(map(lambda item: structure_json(item), data))
    validate = list(map(lambda item: validation(item), clean_data))

    check_validate = list(map(lambda item: result_validate(item), validate))

    ck = list(filter(lambda x: x['code'] != 200, validate))

    if len(ck) > 0:
        return response(400, message=check_validate)

    serializer = departmentSerializers(data=clean_data, many=True)

    try:
        # start transaction
        sid = transaction.savepoint()

        if serializer.is_valid(raise_exception=True):
            serializer.save()

            # save budget
            data_budget = list(filter(lambda x: x['budget'] > 0, data))
            if len(data_budget) > 0:
                clean_datas_budget = list(map(lambda item: clean_budget(item, acc_id), data_budget))

                res_budget = []
                for cldb in clean_datas_budget:
                    store_budget = save_budget(cldb)
                    load_store_budget = json.loads(store_budget.content.decode())

                    res_budget.append(load_store_budget['data'])

            message = "Success create department"
            status = True

            transaction.savepoint_commit(sid)

            res = list(map(lambda item: restructure_json(item), serializer.data))

            return response(201, res, message, status)

    except Exception as e:
        transaction.savepoint_rollback(sid)

        return response(400, message=str(e))


def result_validate(data):

    res = {
        "codeMessage": data['code'],
        "code": data['message']
    }

    return res


def clean_budget(item, acc_id):

    get_department = Department.objects.filter(account=acc_id, code=item['code'].upper()).get()

    data = {
        "departmentId": get_department.id,
        "value": setDefaultValue('budget', item, ''),
        "description": "Penambahan budget",
        "status": "PLUS"
    }

    return data


def validation_bulk_contact_invite(data, acc_id, request):
    # check department exist
    res = {
        "status": False,
        "departmentName": None,
        "message": "Department Not Found"
    }

    try:
        department = Department.objects.filter(id=data['departmentId'], account=acc_id).get()
        if department:
            member_type = department.account.member_type
            # check total email list, max 20
            email_list = data['email']
            if len(email_list) > 20:
                status = False
                department_name = department.name
                message = "Maximum email is 20"

            # check total email less then 1
            elif len(email_list) < 1:
                status = False
                department_name = department.name
                message = "Minimum email is 1"

            else:
                for email in email_list:

                    format_email = validate_email(email)
                    if format_email is False:
                        status = False
                        department_name = department.name
                        message = "Format email is wrong : " + email

                    else:
                        # if email already in contact
                        try:
                            status = True
                            department_name = department.name
                            message = ""
                            contact = Contact.objects.filter(email=email, is_disabled=False)

                            if len(contact) > 0:
                                contact_ids = list(contact.values_list("id", flat=True))

                                acc_contact = AccountContact.objects.filter(account=acc_id, contact_id__in=contact_ids,
                                                                           department=department.id, is_disabled=False)

                                if len(acc_contact) > 0:
                                    status = False
                                    department_name = department.name
                                    message = "Email : {0} Already in choosen department".format(email)

                                else:
                                    is_already_sent = False
                                    account_contact_exist = AccountContact.objects.filter(
                                        account=acc_id, contact_id__in=contact_ids, is_disabled=False)
                                    account_contact = account_contact_exist if len(account_contact_exist) > 0 \
                                        else AccountContact.objects.filter(contact_id__in=contact_ids, is_disabled=False)

                                    contact_list = AccContactSerializer(
                                        account_contact,
                                        many=True).data

                                    for item in contact_list:
                                        if is_already_sent is True:
                                            continue

                                        if item['account'] == acc_id and item['department'] != department.id:
                                            current_department = '-'

                                            if item['department']:
                                                current_department = departmentSerializers(
                                                    Department.objects.get(pk=item['department'])).data['name']

                                            email_data = {
                                                "lastDepartment": current_department,
                                                "newDepartment":
                                                    departmentSerializers(Department.objects.get(pk=department.id)).data['name'],
                                                "email": email,
                                                "memberType": member_type
                                            }

                                            AccountContact.objects.filter(id=item['id'], is_disabled=False).update(
                                                department=department.id)
                                            send_email_change_department(email_data)
                                            status = True
                                            department_name = department.name
                                            message = SUCCESS_INVITE_DEPT
                                            is_already_sent = True

                                        elif item['account'] != acc_id:
                                            inv_data = structure_invitation(item, email)
                                            clean_data = invitationView.structure_json(inv_data)

                                            serializer = invitationSerializers(data=clean_data)

                                            try:
                                                if serializer.is_valid(raise_exception=True):

                                                    serializer.validated_data['created_by'] = get_user_id(
                                                        get_token_data(request._request.META, 'email'))
                                                    serializer.save()

                                                    invitationView.sent_invitation(request, serializer.data, member_type=member_type)
                                                    status = True
                                                    department_name = department.name
                                                    message = SUCCESS_INVITE_DEPT
                                                    is_already_sent = True

                                            except Exception as e:

                                                status = False
                                                department_name = department.name
                                                message = str(e)

                            else:
                                invitation = Invitation.objects.filter(email=email, account=acc_id).get()

                                inv_data = {
                                    "accountId": acc_id,
                                    "departmentId": department.id,
                                    "email": email,
                                }
                                clean_data = invitationView.structure_json(inv_data)

                                if invitation:
                                    serializer = invitationSerializers(invitation, data=clean_data)
                                else:
                                    serializer = invitationSerializers(data=clean_data)

                                try:
                                    if serializer.is_valid(raise_exception=True):

                                        serializer.validated_data['created_by'] = get_user_id(
                                            get_token_data(request._request.META, 'email'))
                                        serializer.save()
                                        invitationView.sent_invitation(request, serializer.data, member_type=member_type)

                                        status = True
                                        department_name = department.name
                                        message = SUCCESS_INVITE_DEPT

                                except Exception as e:
                                    status = False
                                    department_name = department.name
                                    message = str(e)

                        except Exception as e:

                            status = False
                            department_name = department.name
                            message = str(e)

            res = {
                "status": status,
                "departmentName": department_name,
                "message": message
            }
            return res

    except Exception as e:
        res = {
            "status": False,
            "departmentName": None,
            "message": str(e)
        }

        return res


def structure_json_approval(department_id, contact_id, order):
    data = {
        "contact_id": contact_id,
        "department_id": department_id,
        "order": order
    }

    return data


def structure_json_requestor(department_id, contact_id):
    data = {
        "contact_id": contact_id,
        "department_id": department_id
    }

    return data

@method_decorator(csrf_exempt, name='dispatch')
class DeptListAccountView(APIView):
    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def get(self, request):
        query_param = request.GET
        token = get_token_data(self.request.META, key=None, all=True)

        if query_param.get('account_id', ''):
            status, message = self.check_for_IDOR(token, query_param.get('account_id', ''))
            if not status:
                return response(200, data=[], message=message, status=False)

            if all([query_param.get('dept_id',''), query_param.get('unit_id','')]):
                return self.get_subunit_data(query_param.get('unit_id', ''), token)
            return self.get_unit_data(query_param.get('dept_id', ''), token)

        return self.get_account_dept_list(token)

    def get_account_dept_list(self, token):
        response_data = []
        department_ids = []

        if not token.get('accountId', ''):
            return response(200, data=[], message="accountId not detected on token", status=False)

        query = Q(id=token['accountId'])
        if token.get('isAdmin', False):
            query = Q(id=token['accountId']) | Q(parent_id=token['accountId'])
        elif token.get('isApprover', False):
            contact_dept_approval = ContactDepartmentApproval.objects.filter(contact_id=token.get('contactId'))
            department_ids = [item.department_id for item in contact_dept_approval]
        else:
            contact_dept_requestor = ContactDepartmentRequestor.objects.filter(contact_id=token.get('contactId'))
            department_ids = [item.department_id for item in contact_dept_requestor]

        # get main account data
        account_list = Account.objects.origin_query().filter(query)
        for account_data in account_list:
            acc_type = "parent"
            if account_data.parent_id:
                acc_type = "child"
            inner_data = {
                "accountId": account_data.id,
                "accountName": account_data.name,
                "type": acc_type,
                "departments": []
            }
            inner_data['departments'] += self.get_department_data(account_data.id, department_ids)
            if acc_type.lower() == 'child':
                response_data.append(inner_data)
            else:
                response_data.insert(0, inner_data)
        return response(200, data=response_data, message="get account department data success", status=True)

    def get_subunit_data(self, unit_id, token):
        subunit_list = []
        query = Q(type=Department.TYPE_SUBUNIT)

        if token.get('isAdmin', False):
            query &= Q(parent_id__in=unit_id.split(','))
        elif token.get('isApprover', False):
            parent_id_list = [d.parent_id for d in Department.objects.origin_query().filter(id__in=unit_id.split(','))]
            filtered_unit_ids = [f.department_id for f in ContactDepartmentApproval.objects.filter(
                contact_id=token.get('contactId'), department_id__in=parent_id_list)]
            query &= Q(Q(parent_id__in=filtered_unit_ids)|Q(parent_id__parent_id__in=filtered_unit_ids))
        else:
            filtered_unit_ids = [f.department_id for f in ContactDepartmentRequestor.objects.filter(
                contact_id=token.get('contactId'), department_id__in=unit_id.split(','))]
            query &= Q(parent_id__in=filtered_unit_ids)
            query &= Q(id__in=token.get('departmentIds'))

        if not unit_id:
            return response(200, data=[], message="please input valid subunit_id", status=False)


        for subunit_data in Department.objects.origin_query().filter(query):
            subunit_list.append({
                "subunitId": subunit_data.id,
                "subunitName": subunit_data.name
            })
        return response(200, data=subunit_list, message="get unit data success", status=True)

    def get_unit_data(self, dept_id, token):
        if not dept_id:
            return response(200, data=[], message="", status=True)

        if token.get('isAdmin', False):
            token_ids = []
            filtered_dept_ids = dept_id.split(',')
        elif token.get('isApprover', False):
            token_ids = []
            filtered_dept_ids = [f.department_id for f in ContactDepartmentApproval.objects.filter(
                contact_id=token.get('contactId'), department_id__in=dept_id.split(','))]
        else:
            dept_ids_filter = Q(Q(department_id__in=dept_id.split(',')) | Q(department_id__parent_id__in=dept_id.split(',')))
            filtered_dept_ids = [f.department_id for f in ContactDepartmentRequestor.objects.filter(dept_ids_filter)]
            token_ids = token.get('departmentIds')

        unit_list = []
        unit_id_list = []

        if filtered_dept_ids:
            query = Q(parent_id__in=filtered_dept_ids, type=Department.TYPE_UNIT)
            query |= Q(parent_id__parent_id__in=filtered_dept_ids, type=Department.TYPE_SUBUNIT)
        else:
            query = Q(type=Department.TYPE_UNIT)
            query |= Q(type=Department.TYPE_SUBUNIT)

        if token_ids:
            query &= Q(id__in=token_ids)
        for unit_data in Department.objects.origin_query().filter(query):
            if unit_data.type == Department.TYPE_SUBUNIT:
                if unit_data.parent.id not in unit_id_list:
                    unit_list.append({
                        "unitId": unit_data.parent.id,
                        "unitName": unit_data.parent.name
                    })
                    unit_id_list.append(unit_data.parent.id)
            else:
                if unit_data.id not in unit_id_list:
                    unit_list.append({
                        "unitId": unit_data.id,
                        "unitName": unit_data.name
                    })
                    unit_id_list.append(unit_data.id)
        return response(200, data=unit_list, message="get unit data success", status=True)

    def get_department_data(self, account_id, department_ids=[]):
        dept_list = {}
        query = Q(account_id=account_id)
        if department_ids:
            query &= Q(id__in=department_ids)

        for dept_data in Department.objects.origin_query().filter(query):
            if dept_data.type == Department.TYPE_DEPARTMENT:
                if dept_data.id in dept_list: continue
                dept_list[dept_data.id] = {
                    "deptId": dept_data.id,
                    "deptName": dept_data.name
                }
            elif dept_data.type == Department.TYPE_UNIT:
                if dept_data.parent.id in dept_list: continue
                dept_list[dept_data.parent.id] = {
                    "deptId": dept_data.parent.id,
                    "deptName": dept_data.parent.name
                }
            elif dept_data.type == Department.TYPE_SUBUNIT:
                if dept_data.parent.parent.id in dept_list: continue
                dept_list[dept_data.parent.parent.id] = {
                    "deptId": dept_data.parent.parent.id,
                    "deptName": dept_data.parent.parent.name
                }
        return dept_list.values()

    def check_for_IDOR(self, token, account_id):
        if token['iss'] != settings.JWT_ISSUER:
            return True, ""

        account_list = Account.objects.origin_query().filter(
            Q(id=token['accountId']) | Q(parent_id=token['accountId'])).values_list('id', flat=True)

        if not token['accountId'] in account_list:
            return False, "not allowed"

        if not account_id in account_list:
            return False, "not allowed"
        return True, ""

@method_decorator(csrf_exempt, name='dispatch')
class DeptListRequestorView(APIView):
    @partial(loginRequired, module="ACCOUNT", access="MANAGE")
    def get(self, request, dept_id=""):
        result = []
        token = get_token_data(self.request.META, key=None, all=True)

        id_query = request.GET.get('id', '')
        accountId_input = request.GET.get('accountId', '')
        if id_query:
            return self.get_multi_requestors(token, id_query)


        if not dept_id:
            if not token.get('isAdmin', False):
                return response(403, message="not allowed", status=False)

            account_id = token.get('childId') if token.get('childId') else token.get('accountId')

            if accountId_input:
                is_allowed, message = check_account_for_IDOR(token, accountId_input)
                if not is_allowed:
                    return response(403, message="not allowed", status=False)
                account_id = accountId_input

            for requestor_data in ContactDepartmentRequestor.objects.filter(
                    department__account_id=account_id).distinct('contact_id'):
                fullname = [requestor_data.contact.first_name, requestor_data.contact.last_name]
                result.append({
                    "requestor_id": requestor_data.contact.id,
                    "requestor_name": " ".join(fullname),
                    "department_name": requestor_data.department.name
                })
            return response(200, data=result, message="get requestor data success", status=True)

        status, message = self.check_for_IDOR(token, dept_id)
        if not status:
            return response(403, message=message, status=False)

        for requestor_data in ContactDepartmentRequestor.objects.filter(department_id=dept_id).distinct('contact_id'):
            fullname = [requestor_data.contact.first_name, requestor_data.contact.last_name]
            result.append({
                "requestor_id": requestor_data.contact.id,
                "requestor_name": " ".join(fullname),
            })

        meta = {
            "page": 1,
            "limit": 0,
            "totalPages": 1,
            "totalRecords": len(result)
        }
        return response(200, data=result, meta=meta, message="get requestor data success", status=True)


    def get_multi_requestors(self, token, id_query):
        result = []
        id_list = id_query.split(',')

        status, message = self.check_for_IDOR(token, id_list, many=True)
        if not status:
            return response(403, message=message, status=False)

        for requestor_data in ContactDepartmentRequestor.objects.filter(department_id__in=id_list).distinct('contact_id'):
            fullname = [requestor_data.contact.first_name, requestor_data.contact.last_name]
            result.append({
                "requestor_id": requestor_data.contact.id,
                "requestor_name": " ".join(fullname),
            })

        meta_data = {
            "page": 1,
            "limit": 0,
            "totalPages": 1,
            "totalRecords": len(result)
        }
        return response(200, data=result, message="get requestor data success", status=True, meta=meta_data)


    def check_for_IDOR(self, token, dept_id, many=False):
        if token['iss'] != settings.JWT_ISSUER:
            return True, ""

        account_list = Account.objects.origin_query().filter(
            Q(id=token['accountId']) | Q(parent_id=token['accountId'])).values_list('id', flat=True)

        if not many:
            dept_id = Department.objects.origin_query().filter(id=dept_id).first()
            if not dept_id:
                return False, "department not found"

            if not dept_id.account_id in account_list:
                return False, "not allowed"
            return True, ""

        dept_list = Department.objects.origin_query().filter(id__in=dept_id)
        if not dept_list:
            return False, "department not found"

        for data in [dept.account_id for dept in dept_list]:
            if not data in account_list:
                return False, "not allowed"
        return True, ""
