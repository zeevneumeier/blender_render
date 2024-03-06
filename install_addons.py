import bpy

print("installing addons")

#bpy.ops.wm.addon_install(overwrite=True, filepath="/home/ec2-user/blender/FLIP_Fluids_addon.zip")
bpy.ops.preferences.addon_install(overwrite=True, filepath="/home/ec2-user/blender/FLIP_Fluids_addon.zip")
bpy.ops.preferences.addon_enable(module='flip_fluids_addon')

