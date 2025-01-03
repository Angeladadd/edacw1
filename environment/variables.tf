variable img_display_name {
  type = string
  default = "almalinux-9.4-20240805"
}

variable namespace {
  type = string
  default = "ucabc46-comp0235-ns"
}

variable network_name {
  type = string
  default = "ucabc46-comp0235-ns/ds4eng"
}

variable username {
  type = string
  default = "ucabc46"
}

variable keyname {
  type = string
  default = "ucabc46-cnc"
}

variable lecturerkeypath {
  type = string
  default = "lecturer_key.pub"
}

variable worker_count {
  type    = number
  default = 3
}

variable storage_count {
  type    = number
  default = 1
}