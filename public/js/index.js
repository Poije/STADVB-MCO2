function submitSearch () {
    try {
        event.preventDefault ();
        fetch ('/select', {
            method: 'POST',
            body: new URLSearchParams (new FormData (document.getElementById ('searchMovie')))
        })
        .then (response => {
            console.log (response);
            if (response.ok) {
                return response.json ();
            } else {
                throw new Error ("Search failed");
            }
        })
        .then (data => {
            renderResults (data);
        })
        .catch (error => {
            console.log (error);
        });
    } catch (error) {
        console.error (error);
    }
}

function renderResults (results) {
    $.post ('/renderSearch', {results: results}, (html) => {
        $("#resultTable").find ("tr:not(:first)").remove ();
        $('#resultTable')[0].innerHTML += html;
    });
}

function deleteMovie (id) {
    let text = "Would you like to delete movie with id: " + id;
    if (confirm (text) == true) {
        $.post ('/delete', {id: id}, () => {
            window.location.href = "/";
        });
    }
}