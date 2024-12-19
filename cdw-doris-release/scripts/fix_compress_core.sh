#!/bin/bash

# Check if /usr/local/bin/coredump_handler file exists
if [ -f /usr/local/bin/coredump_handler ]; then
    echo "/usr/local/bin/coredump_handler file exists."

    # Check if the file contains a line starting with "exec gzip"
    if grep -q "exec gzip" /usr/local/bin/coredump_handler; then
        echo "The file contains a line starting with 'exec gzip'."

        # Check if pigz tool is already installed
        if ! command -v pigz &> /dev/null; then
            echo "pigz tool is not installed, installing..."

            # Install pigz tool and check if the installation is successful
            if ! yum install -y pigz; then
                echo "Failed to install pigz, exiting script."
                exit 1
            fi
        else
            echo "pigz tool is already installed."
        fi

        # Replace "exec gzip" with "exec pigz"
        echo "Replacing 'exec gzip' with 'exec pigz'..."
        if ! sed -i 's/exec gzip/exec pigz/' /usr/local/bin/coredump_handler; then
            echo "Failed to replace 'exec gzip' with 'exec pigz' in /usr/local/bin/coredump_handler."
            exit 1
        fi

        # Remove lines starting with kernel.core_pattern in /etc/sysctl.conf
        echo "Removing lines starting with 'kernel.core_pattern' in /etc/sysctl.conf..."
        if ! sed -i '/^kernel.core_pattern/d' /etc/sysctl.conf; then
            echo "Failed to remove lines starting with 'kernel.core_pattern' in /etc/sysctl.conf."
            exit 1
        fi

        # Add new kernel.core_pattern configuration at the end of the file
        echo "Adding new 'kernel.core_pattern' configuration at the end of /etc/sysctl.conf..."
        if ! echo "kernel.core_pattern = |/usr/local/bin/coredump_handler" >> /etc/sysctl.conf; then
            echo "Failed to add new 'kernel.core_pattern' configuration to /etc/sysctl.conf."
            exit 1
        fi

        # Reload sysctl configuration
        echo "Reloading sysctl configuration..."
        if ! sysctl -p; then
            echo "Failed to reload sysctl configuration."
            exit 1
        fi

        # Check the content of /proc/sys/kernel/core_pattern
        core_pattern=$(cat /proc/sys/kernel/core_pattern)
        if [ "$core_pattern" == "|/usr/local/bin/coredump_handler" ]; then
            echo "The content of /proc/sys/kernel/core_pattern is correct: $core_pattern"
        else
            echo "The content of /proc/sys/kernel/core_pattern is incorrect: $core_pattern"
            exit 1
        fi

        echo "All operations completed."
    else
        echo "The file does not contain a line starting with 'exec gzip'."
    fi
else
    echo "/usr/local/bin/coredump_handler file does not exist."
fi
