#!/usr/bin/env python3

import argparse
import enum
import htcondor
import os
import random
import signal
import subprocess
import sys
import time

from htcondor import ProvisionerState

PROVISIONER_SUCCESS = 0
PROVISIONER_FAILURE = 1

class EC2Provisioner:

	def __init__(
		self, 
		name = "EC2Annex", 
		expiration = int(time.time() + 300)
	):
		print("Initializing EC2Provisioner object")
		self.resource_id = name + str(random.randint(1, 100000))
		self.resource_expiration = expiration
		self.provisioner_state = ProvisionerState.New
		
		# If we cannot lookup the cluster ID and proc ID, exit immediately
		self.lookup_jobid()
		if self.cluster_id == -1 or self.proc_id == -1:
			print("Could not determine provisioner cluster ID and/or proc ID. Aborting.")
			exit(PROVISIONER_FAILURE)
		print("Provisioner job running with ID {}.{}".format(self.cluster_id, self.proc_id))
		
	def lookup_jobid(self):
		try:
			get_cluster_id = subprocess.Popen("cat $_CONDOR_JOB_AD | grep ClusterId | grep -v Auto | awk '{ print $3 }'", shell=True, stdout=subprocess.PIPE)
			self.cluster_id = get_cluster_id.communicate()[0].decode('utf-8').replace("\n", "")
		except:
			print("Unable to read cluster ID from job ad file")
			self.cluster_id = -1
		try:
			get_proc_id = subprocess.Popen("cat $_CONDOR_JOB_AD | grep ProcId | awk '{ print $3 }'", shell=True, stdout=subprocess.PIPE)
			self.proc_id = get_proc_id.communicate()[0].decode('utf-8').replace("\n", "")
		except:
			print("Unable to read proc ID from job ad file")
			self.proc_id = -1

	def change_state(self, new_state):
		self.provisioner_state = new_state
		
		# TODO: Is there a less ugly way to update the job ad?
		qedit_cmd = "condor_qedit -debug {}.{} ProvisionerState {}".format(self.cluster_id, self.proc_id, new_state.real)
		qedit_process = subprocess.Popen(qedit_cmd, shell=True)
	
		qedit_cmd = "condor_qedit -debug {}.{} ProvisionerResourceID \"\\\"{}\\\"\"".format(self.cluster_id, self.proc_id, self.resource_id)
		qedit_process = subprocess.Popen(qedit_cmd, shell=True)
	
		# TODO: Replace with real event logging via bindings	
		log = open("ec2-provisioner.log", "a")
		log.write("000 (" + str(os.getpid()) + ".000.000) 01/01 00:00:01 Provisioner enter " + str(new_state) + " state on host: <127.0.0.1:38706?addrs=127.0.0.1-38706&alias=localhost&noUDP&sock=schedd_8401_47a0_4>\n")
		log.write("...\n")
		log.close()

	def provision(self, timeout=300):
		"""
		Provisions compute resources on Amazon EC2.

		Returns:
			Boolean: True if resources provisioned correctly, False if any error occurred.
		"""

		print("Provisioning EC2 resources under annex " + str(self.resource_id))
		annex_process = subprocess.Popen("condor_annex -count 1 -annex-name {}".format(self.resource_id), shell=True, stderr=subprocess.PIPE, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
		annex_process_output = annex_process.communicate(input=b'yes')
		
		# Make sure the annex is actually starting correctly; if not then exit
		for line in annex_process_output:
			if b'aborting' in line:
				print("Provisioner failed with the following error: {}".format(annex_process_output[-1]))
				return False

		# Wait for resources to become available
		for i in range(timeout):
			status_output = os.popen("condor_annex -status -annex-name {}".format(self.resource_id)).read()
			if "in-pool" in status_output:
				return True
			time.sleep(1)

		# If we got this far, the provision routine timed out. Return false for failure.
		return False

	def deprovision(self, timeout=120):
		"""
		Terminates provisioned resources on Amazon EC2.

		Returns:
			Boolean: True if resources deprovisioned correctly, False if any error occurred.
		"""

		# Start by deprovisioning EC2 resources using condor_off -annex
		print("Deprovisioning EC2 resources for resource_id = " + str(self.resource_id))
		deprovision_cmd = "condor_off -annex " + str(self.resource_id)
		annex_process = subprocess.Popen(deprovision_cmd, shell=True, stderr=subprocess.PIPE, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

		# Just to be safe, do not exit until resources are flagged as terminated
		for i in range(timeout):
			status_output = os.popen("condor_annex -status -annex-name {}".format(self.resource_id)).read()
			if "terminated" in status_output:
				return True
			time.sleep(1)

		# If we got this far, the deprovision routine timed out. Return false for failure.
		return False

def main():

	print("EC2 Resource Provisioner starting...")

	# Parse input arguments
	parser = argparse.ArgumentParser(description='EC2 resource provisioner')
	parser.add_argument('-state', action="store", dest="state", default=ProvisionerState.New)
	parser.add_argument('-resource_id', action="store", dest="resouce_id", default="")
	parser.add_argument('-expiration', action="store", dest="expiration", type=int, default=0)
	args = parser.parse_args()

	# Setup an initialize the provisioner class object
	ec2 = EC2Provisioner()
	ec2.provisioner_state = args.state
	if args.resouce_id: ec2.resource_id = args.resource_id 
	if args.expiration > 0: ec2.resource_expiration = args.expiration
	
	# Provision EC2 resources.
	print("Current provisioner state is {}".format(ec2.provisioner_state))	
	if ec2.provisioner_state == ProvisionerState.New:
		print("About to start provisioning routine")
		ec2.change_state(ProvisionerState.ProvisioningStarted)
		provision_success = ec2.provision()
		if provision_success is False:
			print("Failed to provision resources, aborting.")
			exit(PROVISIONER_FAILURE)
		ec2.change_state(ProvisionerState.ProvisioningComplete)

	# Now to go to sleep for some time, while waiting for work to happen on provisioned resources
	print("Finished provisioning, now state is {}".format(ec2.provisioner_state))
	if ec2.provisioner_state == ProvisionerState.ProvisioningComplete:
		print("Now waiting for deadline at expiration = {}, current time = {}".format(ec2.resource_expiration, time.time()))
		while time.time() < ec2.resource_expiration:
			#print("Waiting for expiration, time.time() = " + str(time.time()) + ", ec2.resource_expiration = " + str(ec2.resource_expiration))
			time.sleep(1)

	# Deprovision EC2 resources. 
	print("Ready to deprovision, now state is {}".format(ec2.provisioner_state))
	if ec2.provisioner_state == ProvisionerState.ProvisioningComplete:
		ec2.change_state(ProvisionerState.DeprovisioningStarted)
		deprovision_success = ec2.deprovision()
		if deprovision_success is False:
			print("Failed to deprovision resources, exiting.\nPlease deprovision these resources manually to avoid incurring extra costs.")
			exit(PROVISIONER_FAILURE)
		ec2.change_state(ProvisionerState.DeprovisioningComplete)
		
	# All done
	print("All done, now state is {}".format(ec2.provisioner_state))
	if ec2.provisioner_state is ProvisionerState.DeprovisioningComplete:
		print("Looks like everything worked correctly. Exiting.")
		exit(PROVISIONER_SUCCESS)

if __name__ == "__main__":
	main()
