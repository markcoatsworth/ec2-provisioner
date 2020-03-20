#!/usr/bin/env python

from __future__ import print_function

import argparse
import enum
import htcondor
import os
import random
import signal
import subprocess
import sys
import time

PROVISIONER_SUCCESS = 0
PROVISIONER_FAILURE = 1

class ProvisionerState(enum.Enum):
	NONE = "NONE"
	PROVISIONING_ACTIVE = "PROVISIONING ACTIVE"
	PROVISIONING_COMPLETE = "PROVISIONING COMPLETE"
	DEPROVISIONING_ACTIVE = "DEPROVISIONING ACTIVE"
	DEPROVISIONING_COMPLETE = "DEPROVISIONING COMPLETE"

class EC2Provisioner:

	def __init__(
		self, 
		resource_name = "EC2Annex", 
		resource_expiration = int(time.time() + 300)
	):
		print("Initializing EC2Provisioner object")
		self.resource_id = resource_name + str(random.randint(1, 100000))
		self.resource_expiration = resource_expiration
		self.provisioner_state = ProvisionerState.NONE
		
		# If we cannot lookup the cluster ID and proc ID, exit immediately
		self.lookup_jobid()
		if self.cluster_id == -1 or self.proc_id == -1:
			print("Could not determine provisioner cluster ID and/or proc ID. Aborting.")
			#exit(PROVISIONER_FAILURE)
		
	def lookup_jobid(self):
		try:
			get_cluster_id = subprocess.Popen("cat $_CONDOR_JOB_AD | grep ClusterId | grep -v Auto | awk '{ print $3 }'", shell=True, stdout=subprocess.PIPE)
                	self.cluster_id = get_cluster_id.communicate(timeout=1)[0].replace("\n", "")
                except:
			print("Unable to read cluster ID from job ad file")
			self.cluster_id = -1
		try:
			get_proc_id = subprocess.Popen("cat $_CONDOR_JOB_AD | grep ProcId | awk '{ print $3 }'", shell=True, stdout=subprocess.PIPE)
                	self.proc_id = get_proc_id.communicate(timeout=1)[0].replace("\n", "")
		except:
			print("Unable to read proc ID from job ad file")
			self.proc_id = -1

	def change_state(self, new_state):
		self.provisioner_state = new_state
		qedit_cmd = "condor_qedit -debug " + str(self.cluster_id) + "." + str(self.proc_id) + " ProvisionerState " + str(new_state)
		print("qedit_cmd = " + qedit_cmd)
		qedit_process = subprocess.Popen(qedit_cmd, shell=True)
	
		qedit_cmd = "condor_qedit -debug " + str(self.cluster_id) + "." + str(self.proc_id) + " ProvisionerResourceID " + str(self.resource_id)
		print("qedit_cmd = " + qedit_cmd)
		qedit_process = subprocess.Popen(qedit_cmd, shell=True)
		
		log = open("ec2-provisioner.log", "a")
		log.write("000 (" + str(os.getpid()) + ".000.000) 01/01 00:00:01 Provisioner enter " + str(new_state) + " state on host: <127.0.0.1:38706?addrs=127.0.0.1-38706&alias=localhost&noUDP&sock=schedd_8401_47a0_4>\n")
		log.write("...\n")
		log.close()

	def provision(self):
		"""
		Provisions compute resources on Amazon EC2.

		Returns:
			Boolean: True if resources provisioned correctly, False if any error occurred.
		"""

		inpool_column = -1
		provision_timeout = 300

		print("Provisioning EC2 resources under annex " + str(self.resource_id))
		annex_process = subprocess.Popen("condor_annex -count 1 -annex-name " + self.resource_id, shell=True, stderr=subprocess.PIPE, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
		annex_process_output = annex_process.communicate(input="yes")
		
		# Make sure the annex is actually starting correctly; if not then exit
		for line in annex_process_output:
			if "aborting" in line:
				print("Provisioner failed with the following error: " + annex_process_output[-1])
				return False

		# Wait for resources to become available
		for i in range(provision_timeout):
			status_output = os.popen('condor_annex -status').read().splitlines()
			print("Waiting for resources to provision, time = " + str(i) + ", inpool_column = " + str(inpool_column))
			for line in status_output:
				print(line)
				if "NAME" in line and "TOTAL" in line:
					index_columns = line.split()
					for index, column in enumerate(index_columns):
						if column == "in-pool":
							inpool_column = index
							print("Found an in-pool column = " + str(inpool_column))
				if self.resource_id in line:
					print("Found self.resource_id = " + str(self.resource_id))
					line_tokens = line.split()
					if inpool_column > 0 and len(line_tokens) >= inpool_column:
						print("The value of line_tokens[inpool_column] is " + str(line_tokens[inpool_column]))
						if line_tokens[inpool_column].isdigit():
							if int(line_tokens[inpool_column]) > 0:
								print("Resources are available!")
								return True
			time.sleep(1)

		# If we got this far, then condor_annex timed out.
		return False

		# Write an event to the log indicating that resources are ready to go
		# For now, just write a submit event. In the future we'll want a new event to indicate provisioned resources are available
		# Further hackery: pass this job's PID as the cluster number, so we can read it in easily with the JobEventLog class
		#log = open("ec2-provisioner.log", "w")
		#log.write("000 (" + str(os.getpid()) + ".000.000) 01/01 00:00:01 Job submitted from host: <127.0.0.1:38706?addrs=127.0.0.1-38706&alias=localhost&noUDP&sock=schedd_8401_47a0_4>\n")
		log.write("...\n")
		#log.close()

	def deprovision(self):
		"""
		Terminates provisioned resources on Amazon EC2.

		Returns:
			Boolean: True if resources deprovisioned correctly, False if any error occurred.
		"""

		# Start the deprovisioning routine
		deprovision_timeout = 120
		terminated_column = 0

		# Start by deprovisioning EC2 resources using condor_off -annex
		print("Deprovisioning EC2 resources for resource_id = " + str(self.resource_id))
		deprovision_cmd = "condor_off -annex " + str(self.resource_id)
		print("deprovision_cmd = " + str(deprovision_cmd))
		annex_process = subprocess.Popen(deprovision_cmd, shell=True, stderr=subprocess.PIPE, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

		# Just to be safe, do not exit until resources are flagged as terminated
		for i in range(deprovision_timeout):
			status_output = os.popen('condor_annex -status').read().splitlines()
			print("time = " + str(i) + ", terminated_column = " + str(terminated_column))
			for line in status_output:
				print(line)
				if "NAME" in line and "TOTAL" in line:
					index_columns = line.split()
					for index, column in enumerate(index_columns):
						if column == "terminated":
							terminated_column = index
							print("Found a terminated column = " + str(terminated_column))
				if self.resource_id in line:
					line_tokens = line.split()
					#print("len(line_tokens) = " + str(len(line_tokens)))
					if terminated_column > 0 and len(line_tokens) > terminated_column:
						print("The value of line_tokens[terminated_column] is " + str(line_tokens[terminated_column]))
						if line_tokens[terminated_column].isdigit():
							if int(line_tokens[terminated_column]) > 0:
								return True

		# If we got this far, then condor_annex time out. Return false for failured.
		return False

def main():

	print("EC2 Resource Provisioner starting...")

	# Parse input arguments
	parser = argparse.ArgumentParser(description='EC2 resource provisioner')
	parser.add_argument('-state', action="store", dest="state", default=ProvisionerState.NONE)
	parser.add_argument('-resource_id', action="store", dest="resouce_id", default="")
	parser.add_argument('-expiration', action="store", dest="expiration", type=int, default=0)
	args = parser.parse_args()
	
	# Setup an initialize the provisioner class object
	ec2 = EC2Provisioner()
	ec2.provisioner_state = args.state
	if args.resouce_id: ec2.resource_id = args.resource_id 
	if args.expiration > 0: ec2.resource_expiration = args.expiration

	# Provision EC2 resources. This will take 1-3 minutes.
	print("Current provisioner state is " + str(ec2.provisioner_state))	
	if ec2.provisioner_state == ProvisionerState.NONE:
		print("About to start provisioning routine")
		ec2.change_state(ProvisionerState.PROVISIONING_ACTIVE)
		provision_success = ec2.provision()
		if provision_success is False:
			print("Failed to provision resources, aborting.")
			exit(PROVISIONER_FAILURE)
		ec2.change_state(ProvisionerState.PROVISIONING_COMPLETE)

	# Now to go to sleep for some time, while waiting for work to happen on provisioned resources
	print("Finished provisioning, now state is " + str(ec2.provisioner_state))
	if ec2.provisioner_state == ProvisionerState.PROVISIONING_COMPLETE:
		print("Now waiting for deadline at expiration = " + str(ec2.resource_expiration) + ", current time = " + str(time.time()))
		while time.time() < ec2.resource_expiration:
			print("Waiting for expiration, time.time() = " + str(time.time()) + ", ec2.resource_expiration = " + str(ec2.resource_expiration))
			time.sleep(1)

	# Deprovision EC2 resources. 
	print("Ready to deprovision, now state is " + str(ec2.provisioner_state))
	if ec2.provisioner_state == ProvisionerState.PROVISIONING_COMPLETE:
		ec2.change_state(ProvisionerState.DEPROVISIONING_ACTIVE)
		deprovision_success = ec2.deprovision()
		if deprovision_success is False:
			print("Failed to deprovision resources, exiting.\nPlease deprovision these resources manually to avoid incurring extra costs.")
			exit(PROVISIONER_FAILURE)
		ec2.change_state(ProvisionerState.DEPROVISIONING_COMPLETE)
		
	# All done
	print("All done, now state is " + str(ec2.provisioner_state))
	if ec2.provisioner_state is ProvisionerState.DEPROVISIONING_COMPLETE:
		print("Looks like everything worked correctly. Exiting.")
		exit(PROVISIONER_SUCCESS)


if __name__ == "__main__":
	main()
