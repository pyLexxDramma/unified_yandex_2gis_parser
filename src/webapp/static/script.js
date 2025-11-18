function toggleLocationField() {
    var scopeSelect = document.getElementById('search_scope');
    var locationInputContainer = document.getElementById('location_field_container');
    var locationInput = document.getElementById('location');

    if (scopeSelect.value === 'city') {
        locationInputContainer.style.display = 'block';
        locationInput.required = true;
    } else {
        locationInputContainer.style.display = 'none';
        locationInput.required = false;
        locationInput.value = '';
    }
}

document.addEventListener('DOMContentLoaded', function () {
    toggleLocationField();
});

var scopeSelect = document.getElementById('search_scope');
if (scopeSelect) {
    scopeSelect.addEventListener('change', toggleLocationField);
}