# Base image
FROM ubuntu:22.04

# Install necessary packages
RUN apt-get update && apt-get install -y openssh-server

# Create SSH directory and configure SSH
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd

# Configure SSH
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

# Remove check for PAM (optional, depending on your needs)
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

# Add the public key to the authorized_keys file
COPY benchmark_key.pub /root/.ssh/authorized_keys

# Set permissions for the authorized_keys file
RUN chmod 600 /root/.ssh/authorized_keys

# Expose SSH port
EXPOSE 22

# Start SSH service
CMD ["/usr/sbin/sshd", "-D"]
