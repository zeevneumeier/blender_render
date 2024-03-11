echo "installing the latest blender"

export BLENDER_VERSION="blender-4.0.2-linux-x64"
export BLENDER_RELEASE="4.0"
export BLENDER_URL="https://mirror.clarkson.edu/blender/release/Blender$BLENDER_RELEASE/$BLENDER_VERSION.tar.xz"


if [ -d /home/ec2-user/$BLENDER_VERSION ]; then
	echo "Blender already updated. No need to download again"
else
	wget "$BLENDER_URL" -P /home/ec2-user/
	tar -xf /home/ec2-user/$BLENDER_VERSION.tar.xz -C /home/ec2-user/
	ln -s /home/ec2-user/$BLENDER_VERSION /home/ec2-user/blenderapp
fi

if [ ! -d /home/ec2-user/blender ]; then
    mkdir /home/ec2-user/blender
fi

sudo yum install -y libEGL


