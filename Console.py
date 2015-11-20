import Queue
import argparse
import logging
import os
import random
import string
import sys
import threading
import time
from argparse import ArgumentError
from cmd import Cmd
from threading import BoundedSemaphore

from ArubaCloud.PyArubaAPI import CloudInterface
from ArubaCloud.base.Errors import ValidationError
from ArubaCloud.base.logsystem import ArubaLog
from termcolor import cprint

logger = ArubaLog(name=__name__, level=logging.INFO, log_to_file=False)
dc_number = 6

pool = {}
vmw_tq = []
creators_tq = []

vmw_q = Queue.Queue()
creator_q = Queue.Queue()

single_lock = threading.Lock()
vm_creation_sem = BoundedSemaphore(3)


def wait_for_all_jobs_to_finish(dc):
    while len(pool[dc].get_jobs()['Value']) > 0:
        time.sleep(1)
    return True


def loggedin_dc():
    """Return a list of keys that reflect initialized datacenter"""
    logged_in = []
    for dc in pool.keys():
        if pool[dc].is_logged_in() is True:
            logged_in.append(dc)
    return logged_in


def run_async_job(method=None, args=None):
    try:
        for d in loggedin_dc():
            if args is not None:
                if isinstance(method, str):
                    vmw_q.put([getattr(pool[d], method), args])
                elif hasattr(method, '__call__'):
                    vmw_q.put([method, args])
            else:
                if isinstance(method, str):
                    vmw_q.put([getattr(pool[d], method)])
                elif hasattr(method, '__call__'):
                    vmw_q.put([method])
        vmw_q.join()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.debug('Caught Exception: %s in: %s at line: %s\nMsg: %s' % (exc_type, fname, exc_tb.tb_lineno, e))
        raise Exception('Error in run async job')
    return True


class VMWorker(threading.Thread):

    def __init__(self):
        super(VMWorker, self).__init__()
        self.stop = False
        self.logger = logger
        self.logger.name = self.__class__.__name__

    def run(self):
        while self.stop is False:
            try:
                if not vmw_q.empty():
                    items = vmw_q.get(True, 10)
                    try:
                        function = items[0]
                        args = items[1:]
                        if not hasattr(function, '__call__'):
                            raise ValidationError('Function passed to thread is not a function.')
                    except TypeError as e:
                        self.logger.debug('Exception: %s' % e)
                        self.logger.debug('Error retrieving queue command: %s' % items)
                    self.logger.debug('Function: %s args: %s' % (function, args))
                    self.logger.debug('Entering Thread VMWorker with method: %s' % function)
                    if len(args) == 0:
                        function()
                    else:
                        function(*args)
                    self.logger.debug('Called method: %s.' % function)
                    vmw_q.task_done()
                time.sleep(0.25)
            except Queue.Empty:
                pass


class CreatorWorker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop = False
        self.logger = logger
        self.logger.name = self.__class__.__name__

    def run(self):
        while self.stop is False:
            try:
                if not creator_q.empty():
                    vm_type, params = creator_q.get(True, 10)
                    if params.number > 1:
                        rnd = ''.join(random.choice(string.ascii_letters) for _ in range(6))
                        vm_name = '%s-%s' % (params.vmname, rnd)
                    else:
                        vm_name = params.name
                    vm_creation_sem.acquire()
                    # create lock to avoid double creation
                    single_lock.acquire()
                    op = False
                    if vm_type is 'smart':
                        from ArubaCloud.objects import SmartVmCreator
                        creator = SmartVmCreator(name=vm_name,
                                                 admin_password=params.admin_pwd,
                                                 template_id=params.template,
                                                 auth_obj=pool[params.dc].auth
                                                 )
                        creator.set_type(size=params.pkg)
                        op = creator.commit(url=pool[params.dc].wcf_baseurl)
                    elif vm_type is 'pro':
                        from ArubaCloud.objects import ProVmCreator
                        creator = ProVmCreator(name=vm_name,
                                               admin_password=params.adminpwd,
                                               template_id=params.template,
                                               auth_obj=pool[params.dc].auth
                                               )
                        if params.buyip is True:
                            ip = pool[params.dc].purchase_ip()
                            creator.add_public_ip(ip.resid)
                        creator.add_virtual_disk(params.disk1)
                        if params.disk2 > 0:
                            creator.add_virtual_disk(params.disk2)
                        if params.disk3 > 0:
                            creator.add_virtual_disk(params.disk3)
                        if params.disk4 > 0:
                            creator.add_virtual_disk(params.disk4)
                        creator.set_cpu_qty(int(params.cpuqty))
                        creator.set_ram_qty(int(params.ramqty))
                        op = creator.commit(url=pool[params.dc].wcf_baseurl)
                    ret = True if op is True else False
                    single_lock.release()
                    if ret is False:
                        self.logger.warning('Cannot create VM.')
                    else:
                        while pool[params.dc].find_job(vm_name) is not 'JOBNOTFOUND':
                            time.sleep(10)
                        cprint('Creation of VM: %s Done.' % vm_name, 'green')
                    creator_q.task_done()
                    vm_creation_sem.release()
                time.sleep(0.25)
            except Queue.Empty:
                pass


class Datacenter(CloudInterface):

    def __init__(self, dc):
        super(Datacenter, self).__init__(dc)
        self._dc = dc
        self._loggedin = False

    @property
    def dc(self):
        return self._dc

    @dc.setter
    def dc(self, value):
        self._dc = str(value)

    def login(self, username, password, load=True):
        super(Datacenter, self).login(username, password, load)
        self._loggedin = True

    def is_logged_in(self):
        return self._loggedin


# noinspection PyBroadException
class Creator(Cmd, object):

    def __init__(self):
        super(Creator, self).__init__()
        self.prompt = '#(creator)> '
        self.logger = logger

    @staticmethod
    def do_smart(args):
        """Create Smart Server, (use smart -h to obtain help)"""
        smart_parser = argparse.ArgumentParser(prog='smart', add_help=True)
        smart_parser.add_argument('dc', type=str, help='The ID of the datacenter to create on.')
        smart_parser.add_argument('vmname', type=str, help='The name or the base name of the vm[s].')
        smart_parser.add_argument('template', type=str, help='The ID of the template that you want to deploy.')
        smart_parser.add_argument('admin_pwd', type=str, help='The administrator or root password for the vm.')
        smart_parser.add_argument('number', type=int, help='Number of VM that will be created.')
        smart_parser.add_argument('pkg', type=str, help='The ID for the PKG to use: small, medium, large, extralarge.')
        try:
            parsed = smart_parser.parse_args(args.split())
        except:
            return
        print('Creating Smart Server... Wait until the creation is done.')
        print('The process could take some minutes (up to 15), please be patience.')
        if parsed.dc in pool.keys():
            if pool[parsed.dc].is_logged_in() is not True:
                print('You are not logged in in DC: %s. Login before create VM.' % parsed.dc)
                return -1
            setattr(parsed, 'auth', pool[parsed.dc].auth)
            for i in xrange(parsed.number):
                creator_q.put(('smart', parsed))
        else:
            print("You are not logged in in DC: %s. Login before create VM.' % parsed.dc")
            return -1

    @staticmethod
    def do_pro(args):
        """Create Pro Server, (user pro -h to obtain help)"""
        pro_parser = argparse.ArgumentParser(prog='pro', add_help=True)
        pro_parser.add_argument('--dc', type=str, help='The ID of the datacenter to create on.', required=True)
        pro_parser.add_argument('--name', type=str, help='The name or the base name of the vm[s].', required=True)
        pro_parser.add_argument('--template', type=str, help='The ID of the template that you want to deploy.',
                                required=True)
        pro_parser.add_argument('--adminpwd', type=str, help='The administrator or root password for the vm.',
                                required=True)
        pro_parser.add_argument('--cpuqty', type=int, help='Amount of Cores per VM.', default=1)
        pro_parser.add_argument('--ramqty', type=int, help='Amount of RAM GB.', default=1)
        pro_parser.add_argument('--number', type=int, help='Number of VM that will be created.', required=False,
                                default=1)
        pro_parser.add_argument('--disk1', type=int, help='Primary Disk Size.', default=10, required=False)
        pro_parser.add_argument('--disk2', type=int, help='HDD1 Disk Size.', default=0, required=False)
        pro_parser.add_argument('--disk3', type=int, help='HDD2 Disk Size.', default=0, required=False)
        pro_parser.add_argument('--disk4', type=int, help='HDD3 Disk Size.', default=0, required=False)
        pro_parser.add_argument('--buyip', help='Buy 1 Public IP.', default=False, required=False, action='store_true',
                                dest='buyip')
        try:
            parsed = pro_parser.parse_args(args.split())
        except:
            return
        print('Enqueuing Pro Server VM creation.')
        if pool[parsed.dc].is_logged_in() is not True:
            print('You are not logged in in DC: %s. Login before create VM.' % parsed.dc)
            return None
        for i in xrange(parsed.number):
            creator_q.put(('pro', parsed))

    def do_exit(self, args):
        """Exits from the console"""
        return True


# noinspection PyBroadException
class IConsole(Cmd, object):

    def __init__(self):
        Cmd.__init__(self)
        self.prompt = "#> "
        self.intro = "Wellcome to Aruba Cloud Python Console"
        self.logger = logger

    def do_exit(self, args):
        """Exits from the console"""
        return True

    @staticmethod
    def do_create(args):
        """Enter creation menu."""
        sub_cmd = Creator()
        sub_cmd.cmdloop()

    def do_login(self, args):
        """login into a datacenter, login -h for help"""
        parser = argparse.ArgumentParser(prog='login', add_help=True)
        parser.add_argument('--dc', help='Datacenter to login into.', required=True)
        parser.add_argument('--username', help='Username', required=True)
        parser.add_argument('--password', help='Password', required=True)
        try:
            p = parser.parse_args(args.split())
        except:
            return
        try:
            if 'all' in p.dc.lower():
                cprint('Logging in ALL Datacenter!!! WARNING!!!', 'red')
                for _x in xrange(1, 6+1):
                    pool[_x] = Datacenter(str(_x))
                    pool[_x].login(p.username, p.password)
            else:
                pool[p.dc] = Datacenter(p.dc)
                pool[p.dc].login(p.username, p.password)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.debug('Caught Exception: %s in: %s at line: %s\nMsg: %s' %
                              (exc_type, fname, exc_tb.tb_lineno, e))
            self.logger.critical('Error instancing Datacenter Class:\n %s' % e)

    @staticmethod
    def do_showvm(args):
        """Search across logged in datacenter all vm with the name specified name. See showvm -h for help."""
        parser = argparse.ArgumentParser(prog='showvm', add_help=True)
        parser.add_argument('--dc', type=str, help='ID of the datacenter.', required=False)
        parser.add_argument('--name', type=str, help='Pattern String to find.', required=False)
        try:
            p = parser.parse_args(args.split())
        except:
            return
        for dc in loggedin_dc():
            print("Datacenter: {}".format(p.dc))
            for vm in pool[dc].get_vm(pattern=p.name):
                try:
                    from ArubaCloud.objects.VmTypes import Pro, Smart
                    if isinstance(vm, Pro):
                        hds = [
                            ('Size: {} GB'.format(h['Size']), 'ResourceId: {}'.format(h['ResourceId']))
                            for h in vm.hds
                            ]
                        print('VM {} (cores: {}, memory: {} GB, disks: {})\n-> ServerId: {}\n-> TemplateId: {}'.format(
                            vm.vm_name,
                            vm.cpu_qty,
                            vm.ram_qty,
                            hds,
                            vm.sid,
                            vm.template_id
                        ))
                    if isinstance(vm, Smart):
                        print('VM {} (Pkg: {})\n-> ServerId: {}\n->TemplateId: {}'.format(
                            vm.vm_name,
                            vm.package,
                            vm.sid,
                            vm.template_id
                        ))
                except AttributeError:
                    pass

    @staticmethod
    def do_findip(args=None):
        """Search across logged in datacenter the vm that contains the
        passed ip address You can also pass as first argument the dc.
        In this case the program will search only in the specified one.
        """
        parser = argparse.ArgumentParser(prog='findip', add_help=True)
        parser.add_argument('dc', type=str, help='ID of the datacenter.')
        parser.add_argument('ip', type=str, help='The ip address to search for.')
        try:
            parsed = parser.parse_args(args.split())
        except ArgumentError:
            return 0
        if isinstance(parsed.dc, str):
            vm = pool[parsed.dc].vmlist.find_ip(parsed.ip)
            if vm is None:
                print('No VM found with IP: %s' % parsed.ip)
            else:
                print(vm)
        else:
            run_async_job(method='vmlist.find_ip', args=parsed.ip)
            for dc in loggedin_dc():
                for vm in pool[dc].vmlist.last_search_result:
                    if vm[0] is None:
                        print('No VM found in DC: %s, with IP: %s' % (parsed.dc, parsed.ip))
                    else:
                        print(vm[0])

    @staticmethod
    def do_findtemplate(args):
        """
        Search for a specific template. findtemplate -h for help.
        """
        parser = argparse.ArgumentParser(prog='findtemplate', add_help=True)
        parser.add_argument('--template', type=str, help='Name of the template to find', required=True)
        parser.add_argument('--datacenter', type=str, help='Datacenter where to search', required=True)
        parser.add_argument('--hypervisor', type=int, help='Hypervisor ID', required=False, default=None)
        try:
            parsed = parser.parse_args(args.split())
        except:
            return
        dc_obj = pool[parsed.datacenter]
        templates = dc_obj.find_template(name=parsed.template, hv=parsed.hypervisor)
        for template in templates:
            print template
        return

    @staticmethod
    def do_poweroff(args=None):
        """Poweroff a machine. See poweroff -h for help."""
        parser = argparse.ArgumentParser(prog='poweroff', add_help=True)
        parser.add_argument('--dc', type=str, help='ID of the datacenter.', required=True)
        parser.add_argument('--name', type=str, help='The name of the vm(s) to be powered off.', required=True)
        try:
            parsed = parser.parse_args(args.split())
        except:
            return
        if isinstance(parsed.dc, str):
            run_async_job(method='get_vm', args=parsed.name)
            for vm in pool[parsed.dc].vmlist.last_search_result:
                vmw_q.put([vm.poweroff])
            vmw_q.join()
        else:
            run_async_job(method='get_vm', args=parsed.name)
            for dc in loggedin_dc():
                for vm in pool[dc].vmlist.last_search_result:
                    vmw_q.put([vm.poweroff])
            vmw_q.join()

    @staticmethod
    def do_poweron(args=None):
        """Poweron a VM. See poweron -h for help."""
        parser = argparse.ArgumentParser(prog='poweron', add_help=True)
        parser.add_argument('--dc', type=str, help='ID of the datacenter.')
        parser.add_argument('--name', type=str, help='The name of the vm(s) to be powered off.')
        try:
            parsed = parser.parse_args(args.split())
        except:
            return
        if isinstance(parsed.dc, str):
            run_async_job(method='get_vm', args=parsed.name)
            for _ in loggedin_dc():
                for vm in pool[parsed.dc].vmlist.last_search_result:
                    vmw_q.put([vm.poweron])
                vmw_q.join()
        else:
            run_async_job(method='get_vm', args=parsed.vmname)
            for dc in loggedin_dc():
                for vm in pool[dc].vmlist.last_search_result:
                    vmw_q.put([vm.poweron])
            vmw_q.join()

    @staticmethod
    def do_deletevm(args):
        """Delete a Virtual Machine. See deletevm -h for help."""
        parser = argparse.ArgumentParser(prog='deletevm', add_help=True)
        parser.add_argument('--dc', type=str, help='ID of the datacenter.')
        parser.add_argument('--name', type=str,
                            help='The name of the vm(s) to be powered off.'
                                 'WARNING: If you have two or more VM named for example: testvm, testvm2, testvm3 and'
                                 'you do:'
                                 'deletevm --dc 2 --name testvm, ALL OF THE MACHINE NAMED TESTVM* WILL BE POWERED OFF'
                                 'AND DELETED!!!!'
                            )
        try:
            p = parser.parse_args(args.split())
        except:
            return
        run_async_job(method='get_vm', args=p.name)
        cprint('Powering off VM...', 'yellow')
        for dc in loggedin_dc():
            for vm in pool[dc].vmlist.last_search_result:
                vmw_q.put([pool[dc].poweroff_server, None, vm.sid])
        vmw_q.join()
        cprint('Waiting for VM(s) to poweroff...', 'yellow')
        run_async_job(method=wait_for_all_jobs_to_finish, args=p.dc)
        cprint('Deleting VM...', 'red')
        for dc in loggedin_dc():
            for vm in pool[dc].vmlist.last_search_result:
                vmw_q.put([pool[dc].delete_vm, None, vm.sid])
        vmw_q.join()
        # update internal server list
        run_async_job(method='get_servers')

    """
    @staticmethod
    def do_buy_ip(args):
        if not isinstance(args, int) or isinstance(args, str):
            raise ValidationError('No Datacenter Specified.')
        ip = pool[args].buy_ip()
        print(ip)
    """


if __name__ == '__main__':
    for x in xrange(1, dc_number+1):
        logger.debug('Starting Worker Thread: %s' % x)
        vmw_t = VMWorker()
        vmw_tq.append(vmw_t)
        vmw_t.setDaemon(True)
        vmw_t.start()
    for cr in xrange(3):
        logger.debug('Starting Creator Thread: %s' % cr)
        c = CreatorWorker()
        creators_tq.append(c)
        c.setDaemon(True)
        c.start()

    console = IConsole()
    console.cmdloop()

    for t in vmw_tq:
        t.stop = True
    for t in creators_tq:
        t.stop = True
