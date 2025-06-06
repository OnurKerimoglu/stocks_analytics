resource "google_compute_instance" "default" {
  name         = var.service_name
  machine_type = var.machine_type
  zone         = var.zone

  tags = var.tags

  boot_disk {
    initialize_params {
      image = var.boot_disk_image
      type  = var.boot_disk_type
      size  = var.boot_disk_size
    }
  }

  network_interface {
    network = google_compute_network.this.id

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = var.metadata

  metadata_startup_script = var.metadata_startup_script

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = var.google_service_account_email
    scopes = ["cloud-platform"]
  }
  provisioner "file" {
    source      = "${var.gcp_key_path_src}"
    destination = "${var.gcp_key_path_dest}"
  }

  # Copy init.sh to VM and run it once
  provisioner "file" {
    source      = "${var.init_script_path}"
    destination = "init.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x init.sh",
      "sudo ./init.sh"
    ]
  }

  connection {
    type        = "ssh"
    user        = "${var.ssh_user_1}"
    private_key = "${file(var.private_key_path_1)}"
    host        = self.network_interface[0].access_config[0].nat_ip
  }
}