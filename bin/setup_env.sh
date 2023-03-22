#. bin/setup_env.sh

# Install the virtualenv
pip install virtualenv

activate() {
    # Create the virtual environment
    virtualenv env
    
    # Activate the virtual environment 
    source env/bin/activate

    # install python libraries
    echo "install requirements to virtual environment"
    pip install -r requirements.txt
}
activate