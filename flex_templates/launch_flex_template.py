"""
Entrypoint for Dataflow Flex Template.
This will be referenced in the Dockerfile via the ENV variable FLEX_TEMPLATE_PYTHON_PY_FILE.
"""
import sys
import traceback

try:
    from flex_templates.launcher import launch_pipeline
    launch_pipeline(sys.argv[1:])
except Exception as e:
    print("Launcher failed with error:", str(e))
    traceback.print_exc()
    sys.exit(1)