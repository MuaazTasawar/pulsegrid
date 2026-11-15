# rclone settings for off-server backups

Put your `rclone.conf` in this directory (the file name must be exactly
`rclone.conf`). It holds storage credentials and the encryption password, so
it is git-ignored and must never be committed. See "Backups" in
`deploy/PRODUCTION.md` for how to create it and switch the off-server copy on.
