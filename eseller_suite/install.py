import click

from eseller_suite.setup import after_install as setup


def after_install():
	try:
		print("Setting up eSeller Suite...")
		setup()

		click.secho("Thank you for installing eSeller Suite!", fg="green")

	except Exception as e:
		BUG_REPORT_URL = "https://github.com/efeone/eseller_suite/issues/new"
		click.secho(
			"Installation for eSeller Suite app failed due to an error."
			" Please try re-installing the app or"
			f" report the issue on {BUG_REPORT_URL} if not resolved.",
			fg="bright_red",
		)
		raise e
