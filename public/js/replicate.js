// Function to execute the POST request
const executePostRequest = async () => {
    try {
        const response = await fetch('/replicate', {
            method: 'POST',
        })
        .then (response => {
            console.log (response);
            if (response.ok) {
                return response.text ();
            } else {
                throw new Error ("Replication failed - Front");
            }
        })
        .then (data => {
            console.log ("Status: " + data);
        })
        .catch (error => {
            console.log (error);
        });
    } catch (error) {
        console.error(error);
    }
};

// Call the function immediately when the page loads
executePostRequest();

// Set interval to call the function every 10 seconds (10000 milliseconds)
setInterval(() => {
    executePostRequest();
}, 10000);
