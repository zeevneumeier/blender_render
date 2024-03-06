import bpy


print("Running custom configuration")

# https://blender.stackexchange.com/questions/5281/blender-sets-compute-device-cuda-but-doesnt-use-it-for-actual-render-on-ec2
bpy.context.preferences.addons['cycles'].preferences.compute_device_type = 'CUDA'
#bpy.context.preferences.addons['cycles'].preferences.devices[0].use = True
for device in bpy.context.preferences.addons['cycles'].preferences.devices:
    device.use = True

bpy.context.scene.cycles.device = 'GPU'
#bpy.context.scene.cycles.device = 'CPU'

#for scene in bpy.data.scenes:
#    scene.render.tile_x = 192
#    scene.render.tile_y = 108

#bpy.data.scenes["william"].render.filepath = "/tmp/output.png"
#bpy.ops.render.render(write_still=True)

render_path = bpy.context.scene.render.filepath
while render_path.endswith("#"):
    render_path = render_path[:-1]

if not render_path.endswith("/"):
    render_path = render_path + "/"

print("now changing all the file output nodes (if any) to match the render path", render_path)



for node in bpy.context.scene.node_tree.nodes:
    if isinstance(node, bpy.types.CompositorNodeOutputFile):
        print("Found file output node with path", node.base_path)
        node.base_path = render_path
